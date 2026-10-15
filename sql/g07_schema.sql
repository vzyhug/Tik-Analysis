CREATE SCHEMA IF NOT EXISTS vitied;

CREATE TABLE IF NOT EXISTS vitied.publication (
    publication_id text PRIMARY KEY,
    dataset_id text NOT NULL,
    dataset_version text NOT NULL,
    gold_schema_version text NOT NULL,
    gold_path text NOT NULL,
    source_silver_path text NOT NULL,
    graph_version text NOT NULL,
    published_at_utc timestamptz NOT NULL,
    loaded_at_utc timestamptz NOT NULL DEFAULT now(),
    timezone text NOT NULL,
    topic_label_status text NOT NULL,
    sound_status text NOT NULL,
    is_current boolean NOT NULL DEFAULT false
);
CREATE UNIQUE INDEX IF NOT EXISTS vitied_one_current_publication
    ON vitied.publication (is_current) WHERE is_current;

CREATE TABLE IF NOT EXISTS vitied.post_period (
    published_date date NOT NULL,
    media_type text NOT NULL,
    post_count bigint NOT NULL CHECK (post_count >= 0),
    dataset_id text NOT NULL,
    dataset_version text NOT NULL,
    publication_id text NOT NULL REFERENCES vitied.publication(publication_id),
    aggregation_version text NOT NULL,
    timezone text NOT NULL,
    PRIMARY KEY (publication_id, published_date, media_type)
);
CREATE TABLE IF NOT EXISTS vitied.hashtag_period (
    published_date date NOT NULL,
    tag_key text NOT NULL,
    post_count bigint NOT NULL CHECK (post_count >= 0),
    dataset_id text NOT NULL,
    dataset_version text NOT NULL,
    publication_id text NOT NULL REFERENCES vitied.publication(publication_id),
    aggregation_version text NOT NULL,
    timezone text NOT NULL,
    PRIMARY KEY (publication_id, published_date, tag_key)
);
CREATE TABLE IF NOT EXISTS vitied.hashtag_pair_period (
    published_date date NOT NULL,
    tag_key_1 text NOT NULL,
    tag_key_2 text NOT NULL,
    post_count bigint NOT NULL CHECK (post_count >= 0),
    dataset_id text NOT NULL,
    dataset_version text NOT NULL,
    publication_id text NOT NULL REFERENCES vitied.publication(publication_id),
    aggregation_version text NOT NULL,
    timezone text NOT NULL,
    PRIMARY KEY (publication_id, published_date, tag_key_1, tag_key_2)
);
ALTER TABLE vitied.hashtag_pair_period DROP CONSTRAINT IF EXISTS hashtag_pair_period_check;
DO $pair_order_constraint$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'vitied.hashtag_pair_period'::regclass
          AND conname = 'hashtag_pair_period_unicode_order_check'
    ) THEN
        ALTER TABLE vitied.hashtag_pair_period
            ADD CONSTRAINT hashtag_pair_period_unicode_order_check
            CHECK (tag_key_1 COLLATE "C" < tag_key_2 COLLATE "C");
    END IF;
END
$pair_order_constraint$;
CREATE TABLE IF NOT EXISTS vitied.topic_period (
    published_date date NOT NULL,
    topic_id text NOT NULL,
    post_count bigint NOT NULL CHECK (post_count >= 0),
    label_source text NOT NULL,
    dataset_version text NOT NULL,
    publication_id text NOT NULL REFERENCES vitied.publication(publication_id),
    PRIMARY KEY (publication_id, published_date, topic_id, label_source)
);
CREATE TABLE IF NOT EXISTS vitied.topic_hashtag (
    published_date date NOT NULL,
    topic_id text NOT NULL,
    tag_key text NOT NULL,
    post_count bigint NOT NULL CHECK (post_count >= 0),
    label_source text NOT NULL,
    dataset_version text NOT NULL,
    publication_id text NOT NULL REFERENCES vitied.publication(publication_id),
    PRIMARY KEY (publication_id, published_date, topic_id, tag_key, label_source)
);
CREATE TABLE IF NOT EXISTS vitied.topic_sound (
    published_date date NOT NULL,
    topic_id text NOT NULL,
    sound_id text NOT NULL,
    post_count bigint NOT NULL CHECK (post_count >= 0),
    label_source text NOT NULL,
    dataset_version text NOT NULL,
    publication_id text NOT NULL REFERENCES vitied.publication(publication_id),
    PRIMARY KEY (publication_id, published_date, topic_id, sound_id, label_source)
);
CREATE TABLE IF NOT EXISTS vitied.engagement_observation (
    post_id text NOT NULL,
    observed_at_utc timestamptz NOT NULL,
    published_at_utc timestamptz NOT NULL,
    view_count bigint,
    like_count bigint,
    comment_count bigint,
    share_count bigint,
    save_count bigint,
    media_type text NOT NULL,
    observed_date date NOT NULL,
    dataset_id text NOT NULL,
    dataset_version text NOT NULL,
    publication_id text NOT NULL REFERENCES vitied.publication(publication_id),
    schema_version text NOT NULL,
    timezone text NOT NULL,
    PRIMARY KEY (publication_id, post_id, observed_at_utc),
    CHECK (view_count IS NULL OR view_count >= 0),
    CHECK (like_count IS NULL OR like_count >= 0),
    CHECK (comment_count IS NULL OR comment_count >= 0),
    CHECK (share_count IS NULL OR share_count >= 0),
    CHECK (save_count IS NULL OR save_count >= 0)
);
CREATE TABLE IF NOT EXISTS vitied.coverage (
    published_date date NOT NULL,
    media_type text NOT NULL,
    post_count bigint NOT NULL CHECK (post_count >= 0),
    posts_with_caption_first_observation bigint NOT NULL CHECK (posts_with_caption_first_observation >= 0),
    posts_with_hashtag_first_observation bigint NOT NULL CHECK (posts_with_hashtag_first_observation >= 0),
    observation_count bigint NOT NULL CHECK (observation_count >= 0),
    view_observation_count bigint NOT NULL CHECK (view_observation_count >= 0),
    like_observation_count bigint NOT NULL CHECK (like_observation_count >= 0),
    comment_observation_count bigint NOT NULL CHECK (comment_observation_count >= 0),
    share_observation_count bigint NOT NULL CHECK (share_observation_count >= 0),
    save_observation_count bigint NOT NULL CHECK (save_observation_count >= 0),
    dataset_id text NOT NULL,
    dataset_version text NOT NULL,
    publication_id text NOT NULL REFERENCES vitied.publication(publication_id),
    schema_version text NOT NULL,
    timezone text NOT NULL,
    PRIMARY KEY (publication_id, published_date, media_type)
);

CREATE INDEX IF NOT EXISTS vitied_post_period_date_idx ON vitied.post_period (published_date, media_type);
CREATE INDEX IF NOT EXISTS vitied_hashtag_period_tag_idx ON vitied.hashtag_period (tag_key, published_date);
CREATE INDEX IF NOT EXISTS vitied_engagement_observed_idx ON vitied.engagement_observation (observed_date, media_type);

CREATE OR REPLACE VIEW vitied.vw_post_period AS
SELECT f.* FROM vitied.post_period f JOIN vitied.publication p USING (publication_id) WHERE p.is_current;
CREATE OR REPLACE VIEW vitied.vw_hashtag_period AS
SELECT f.* FROM vitied.hashtag_period f JOIN vitied.publication p USING (publication_id) WHERE p.is_current;
CREATE OR REPLACE VIEW vitied.vw_hashtag_pair_period AS
SELECT f.* FROM vitied.hashtag_pair_period f JOIN vitied.publication p USING (publication_id) WHERE p.is_current;
CREATE OR REPLACE VIEW vitied.vw_topic_period AS
SELECT f.* FROM vitied.topic_period f JOIN vitied.publication p USING (publication_id) WHERE p.is_current;
CREATE OR REPLACE VIEW vitied.vw_topic_hashtag AS
SELECT f.* FROM vitied.topic_hashtag f JOIN vitied.publication p USING (publication_id) WHERE p.is_current;
CREATE OR REPLACE VIEW vitied.vw_topic_sound AS
SELECT f.* FROM vitied.topic_sound f JOIN vitied.publication p USING (publication_id) WHERE p.is_current;
CREATE OR REPLACE VIEW vitied.vw_engagement_observation AS
SELECT f.* FROM vitied.engagement_observation f JOIN vitied.publication p USING (publication_id) WHERE p.is_current;
CREATE OR REPLACE VIEW vitied.vw_coverage AS
SELECT f.* FROM vitied.coverage f JOIN vitied.publication p USING (publication_id) WHERE p.is_current;
CREATE OR REPLACE VIEW vitied.vw_publication AS
SELECT publication_id,dataset_id,dataset_version,gold_schema_version,gold_path,source_silver_path,
       graph_version,published_at_utc,loaded_at_utc,timezone,topic_label_status,sound_status,is_current
FROM vitied.publication WHERE is_current;
