package vn.bigdata.topic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

/** Small full-batch contract for synchronous, sample-weighted two-worker training. */
public final class DistributedAutoencoder {
    private DistributedAutoencoder() {}

    private record Row(String postId, float[] vector) {}

    private static List<Row> read(Path path, int dimension) throws IOException {
        List<Row> rows = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t", 2);
                if (fields.length != 2 || !fields[0].matches("[0-9]{18,20}")) {
                    throw new IllegalArgumentException("Invalid feature row key");
                }
                String[] parts = fields[1].split(",", -1);
                if (parts.length != dimension) {
                    throw new IllegalArgumentException("Feature dimension mismatch for " + fields[0]);
                }
                float[] vector = new float[dimension];
                for (int i = 0; i < dimension; i++) {
                    vector[i] = Float.parseFloat(parts[i]);
                    if (!Float.isFinite(vector[i])) {
                        throw new IllegalArgumentException("Non-finite feature");
                    }
                }
                rows.add(new Row(fields[0], vector));
            }
        }
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Empty feature shard: " + path);
        }
        return rows;
    }

    private static INDArray matrix(List<Row> rows, int start, int end, int dimension) {
        float[][] values = new float[end - start][dimension];
        for (int i = start; i < end; i++) {
            values[i - start] = rows.get(i).vector();
        }
        return Nd4j.create(values);
    }

    private static MultiLayerNetwork newNetwork(int dimension, long seed) {
        var configuration = new NeuralNetConfiguration.Builder()
            .seed(seed).updater(new Adam(0.003)).weightInit(WeightInit.XAVIER)
            .list()
            .layer(new DenseLayer.Builder().nIn(dimension).nOut(128).activation(Activation.RELU).build())
            .layer(new DenseLayer.Builder().nIn(128).nOut(64).activation(Activation.TANH).build())
            .layer(new DenseLayer.Builder().nIn(64).nOut(128).activation(Activation.RELU).build())
            .layer(new OutputLayer.Builder(LossFunctions.LossFunction.MSE)
                   .nIn(128).nOut(dimension).activation(Activation.IDENTITY).build())
            .build();
        MultiLayerNetwork network = new MultiLayerNetwork(configuration);
        network.init();
        return network;
    }

    private static int dimension(MultiLayerNetwork network) {
        return (int) ((DenseLayer) network.getLayerWiseConfigurations().getConf(0).getLayer()).getNIn();
    }

    private static void init(String[] args) throws IOException {
        int dimension = Integer.parseInt(args[1]);
        long seed = Long.parseLong(args[2]);
        MultiLayerNetwork network = newNetwork(dimension, seed);
        ModelSerializer.writeModel(network, Path.of(args[3]).toFile(), false);
        System.out.println("initialized dimension=" + dimension + " parameters=" + network.numParams());
    }

    private static void worker(String[] args) throws IOException {
        MultiLayerNetwork network = ModelSerializer.restoreMultiLayerNetwork(Path.of(args[1]).toFile(), false);
        int dim = dimension(network);
        List<Row> rows = read(Path.of(args[2]), dim);
        int batchSize = Integer.parseInt(args[5]);
        int round = Integer.parseInt(args[6]);
        if (batchSize < 1 || batchSize > 1024) {
            throw new IllegalArgumentException("Invalid batch size");
        }
        String host = java.net.InetAddress.getLocalHost().getHostName();
        int batches = (rows.size() + batchSize - 1) / batchSize;
        INDArray before = network.params().dup();
        long started = System.nanoTime();
        System.out.printf("event=worker_start host=%s round=%d samples=%d batch_size=%d batches=%d%n",
            host, round, rows.size(), batchSize, batches);
        System.out.flush();
        int batch = 0;
        for (int start = 0; start < rows.size(); start += batchSize) {
            INDArray input = matrix(rows, start, Math.min(start + batchSize, rows.size()), dim);
            network.fit(new DataSet(input, input.dup()));
            batch++;
            if (batch == 1 || batch == batches || batch % 10 == 0) {
                double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
                System.out.printf(Locale.ROOT,
                    "event=worker_batch host=%s round=%d local_epoch=1 batch=%d/%d samples=%d score=%.8f elapsed_seconds=%.1f%n",
                    host, round, batch, batches, rows.size(), network.score(), elapsed);
                System.out.flush();
            }
        }
        ModelSerializer.writeModel(network, Path.of(args[3]).toFile(), false);
        double delta = network.params().sub(before).norm2Number().doubleValue();
        double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
        String result = "{\"samples\":" + rows.size() + ",\"batches\":" + batches +
            ",\"host\":\"" + host + "\",\"parameter_delta_l2\":" + delta +
            ",\"training_seconds\":" + elapsed + "}";
        Files.writeString(Path.of(args[4]), result + "\n", StandardCharsets.UTF_8);
        System.out.printf(Locale.ROOT,
            "event=worker_complete host=%s round=%d samples=%d batches=%d parameter_delta_l2=%.8f training_seconds=%.1f model=%s%n",
            host, round, rows.size(), batches, delta, elapsed, args[3]);
        System.out.flush();
    }

    private static void average(String[] args) throws IOException {
        MultiLayerNetwork left = ModelSerializer.restoreMultiLayerNetwork(Path.of(args[1]).toFile(), false);
        int leftCount = Integer.parseInt(args[2]);
        MultiLayerNetwork right = ModelSerializer.restoreMultiLayerNetwork(Path.of(args[3]).toFile(), false);
        int rightCount = Integer.parseInt(args[4]);
        if (leftCount < 1 || rightCount < 1 || left.numParams() != right.numParams()) {
            throw new IllegalArgumentException("Invalid aggregation counts or parameter shapes");
        }
        INDArray parameters = left.params().dup().muli(leftCount)
            .addi(right.params().mul(rightCount)).divi(leftCount + rightCount);
        left.setParams(parameters);
        ModelSerializer.writeModel(left, Path.of(args[5]).toFile(), false);
        System.out.println("averaged samples=" + (leftCount + rightCount));
    }

    private static void evaluate(String[] args) throws IOException {
        MultiLayerNetwork network = ModelSerializer.restoreMultiLayerNetwork(Path.of(args[1]).toFile(), false);
        int dim = dimension(network);
        List<Row> rows = read(Path.of(args[2]), dim);
        double error = 0.0;
        double zeroError = 0.0;
        for (int start = 0; start < rows.size(); start += 64) {
            INDArray input = matrix(rows, start, Math.min(start + 64, rows.size()), dim);
            INDArray output = network.output(input, false);
            error += output.sub(input).mul(output.sub(input)).sumNumber().doubleValue();
            zeroError += input.mul(input).sumNumber().doubleValue();
        }
        double mse = error / (rows.size() * dim);
        double baseline = zeroError / (rows.size() * dim);
        String result = "{\"samples\":" + rows.size() + ",\"mse\":" + mse +
            ",\"zero_mse\":" + baseline + "}";
        Files.writeString(Path.of(args[3]), result + "\n", StandardCharsets.UTF_8);
        System.out.println(result);
    }

    private static void embed(String[] args) throws IOException {
        MultiLayerNetwork network = ModelSerializer.restoreMultiLayerNetwork(Path.of(args[1]).toFile(), false);
        int dim = dimension(network);
        List<Row> rows = read(Path.of(args[2]), dim);
        try (BufferedWriter writer = Files.newBufferedWriter(Path.of(args[3]), StandardCharsets.UTF_8)) {
            for (int start = 0; start < rows.size(); start += 64) {
                int end = Math.min(start + 64, rows.size());
                INDArray hidden = network.feedForward(matrix(rows, start, end, dim), false).get(2);
                if (hidden.columns() != 64) {
                    throw new IllegalStateException("Embedding dimension is not 64");
                }
                for (int row = start; row < end; row++) {
                    writer.write(rows.get(row).postId() + "\t");
                    for (int col = 0; col < 64; col++) {
                        if (col > 0) writer.write(',');
                        writer.write(Float.toString(hidden.getFloat(row - start, col)));
                    }
                    writer.newLine();
                }
            }
        }
        System.out.println("embedded samples=" + rows.size());
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("Required command: init/worker/average/evaluate/embed");
        switch (args[0]) {
            case "init" -> init(args);
            case "worker" -> worker(args);
            case "average" -> average(args);
            case "evaluate" -> evaluate(args);
            case "embed" -> embed(args);
            default -> throw new IllegalArgumentException("Unknown command " + args[0]);
        }
    }
}
