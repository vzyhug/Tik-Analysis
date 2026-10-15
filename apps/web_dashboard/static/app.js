const byId = (id) => document.getElementById(id);
const fmt = (n, digits = 0) => n == null ? '—' : new Intl.NumberFormat('vi-VN', { maximumFractionDigits: digits }).format(n);
const pct = (n) => n == null ? '—' : new Intl.NumberFormat('vi-VN', { style: 'percent', maximumFractionDigits: 1 }).format(n);
const safe = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const colors = ['#27695e','#dc7048','#5379a5','#b38a36','#866a9b','#468a9b','#ad5f73','#78925d'];

function theme(row) {
  if (row.cluster < 0) return 'Chưa nhận diện';
  const words = (row.caption_terms || []).join(' ').toLocaleLowerCase('vi');
  if (/váy|phối|outfit|mặc|áo|quần|thời trang/.test(words)) return 'Thời trang & phối đồ';
  if (/bánh|kẹo|bún|ăn|món|ngon|unbox/.test(words)) return 'Ẩm thực & khám phá sản phẩm';
  if (/giúp con|sung|kem chống|chống nắng/.test(words)) return 'Chăm sóc gia đình & chống nắng';
  if (/tóc|nails|da|kem|makeup|mỹ phẩm|đẹp/.test(words)) return 'Làm đẹp & chăm sóc';
  if (/trả lời|hỏi|đáp|comment/.test(words)) return 'Hỏi đáp & phản hồi';
  if (/biển|du lịch|đi chơi|check in|địa điểm|xinh|mùa/.test(words)) return 'Phong cách & khám phá';
  if (/nhảy|dance|nhạc|trend/.test(words)) return 'Giải trí & biểu diễn';
  if (/lắm luôn|lúc nào|bao nhiêu|nhiêu|mấy|sài/.test(words)) return 'Chia sẻ đời sống';
  return (row.caption_terms || []).slice(0, 2).join(' · ') || 'Nhóm nội dung khác';
}
function groupName(cluster, rows) { return theme(rows.find((r) => r.cluster === cluster) || {cluster, caption_terms: []}); }
function empty(id, message) { byId(id).innerHTML = `<div class="empty-chart">${safe(message)}</div>`; }

function changeBars(rows) {
  const shown = rows.filter((r) => r.cluster >= 0).sort((a,b) => a.share_change_pp-b.share_change_pp);
  if (!shown.length) return empty('change-chart', 'Chưa đủ dữ liệu để so sánh giai đoạn');
  const max = Math.max(.2, ...shown.map((r) => Math.abs(r.share_change_pp)));
  byId('change-chart').innerHTML = shown.map((r) => {
    const v=r.share_change_pp, width=Math.max(2,Math.abs(v)/max*100), positive=v>=0;
    return `<div class="signal-bar"><span class="signal-name">${safe(groupName(r.cluster, rows))}</span><span class="signal-track ${positive?'gain':'loss'}"><i style="width:${width}%"></i></span><b class="${positive?'gain-text':'loss-text'}">${v>0?'+':''}${fmt(v,1)} đpt</b></div>`;
  }).join('');
}

function breadthPlot(rows) {
  const shown=rows.filter((r)=>r.cluster>=0);
  if(!shown.length) return empty('breadth-chart','Chưa đủ dữ liệu creator để đánh giá độ lan rộng');
  const W=560,H=230,L=48,R=18,T=15,B=42,xmax=Math.max(1,...shown.map(r=>r.creators_recent)), ymin=Math.min(-1,...shown.map(r=>r.share_change_pp)), ymax=Math.max(1,...shown.map(r=>r.share_change_pp));
  const x=(v)=>L+v/xmax*(W-L-R), y=(v)=>T+(ymax-v)/(ymax-ymin||1)*(H-T-B), zero=y(0);
  const pts=shown.map((r)=>{const xx=x(r.creators_recent), yy=y(r.share_change_pp), label=groupName(r.cluster,rows);return `<circle cx="${xx}" cy="${yy}" r="6" fill="${colors[r.cluster%colors.length]}" stroke="white" stroke-width="2"><title>${safe(label)} · ${fmt(r.creators_recent)} creator · ${fmt(r.share_change_pp,1)} điểm phần trăm</title></circle><text class="plot-label" x="${xx+8}" y="${yy-7}">${safe(label)}</text>`}).join('');
  const grid=[0,.5,1].map(f=>{const yy=T+f*(H-T-B),val=ymax-f*(ymax-ymin);return `<line class="grid-line" x1="${L}" x2="${W-R}" y1="${yy}" y2="${yy}"/><text class="axis-label" x="${L-8}" y="${yy+3}" text-anchor="end">${fmt(val,1)}</text>`}).join('');
  byId('breadth-chart').innerHTML=`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Creator tham gia và thay đổi thị phần">${grid}<line class="zero-line" x1="${L}" x2="${W-R}" y1="${zero}" y2="${zero}"/>${pts}<text class="axis-label" x="${(L+W-R)/2}" y="${H-8}" text-anchor="middle">Creator đăng trong 4 tuần gần nhất</text></svg>`;
}

function forecastChart(data, rows) {
  if(!data?.ready) { byId('forecast-legend').textContent=''; return empty('forecast-chart',data?.reason || 'Chưa đủ dữ liệu cho kịch bản 4 tuần'); }
  const chosen=data.momentum.filter(r=>r.cluster>=0&&r.share_recent>0).sort((a,b)=>b.share_recent-a.share_recent).slice(0,4);
  const periods=[...new Set([...data.history.map(x=>x.period),...data.forecast_candidates.map(x=>x.period)])];
  const W=660,H=240,L=48,R=15,T=14,B=37, hist=new Map(data.history.map(r=>[`${r.period}|${r.cluster}`,r.share])), fc=new Map(data.forecast_candidates.map(r=>[`${r.period}|${r.cluster}`,r]));
  const max=Math.max(.1,...chosen.flatMap(c=>periods.map(p=>Math.max(hist.get(`${p}|${c.cluster}`)||0,fc.get(`${p}|${c.cluster}`)?.flat||0))))*1.13;
  const x=i=>L+i*(W-L-R)/(periods.length-1||1), y=v=>T+(max-v)*(H-T-B)/max;
  const grid=[0,.5,1].map(f=>{const v=max*(1-f),yy=y(v);return `<line class="grid-line" x1="${L}" x2="${W-R}" y1="${yy}" y2="${yy}"/><text class="axis-label" x="${L-8}" y="${yy+3}" text-anchor="end">${fmt(v*100)}%</text>`}).join('');
  const paths=chosen.map(c=>{const color=colors[c.cluster%colors.length], points=data.history.filter(r=>r.cluster===c.cluster), last=points.at(-1), future=data.forecast_candidates.filter(r=>r.cluster===c.cluster); if(!last)return ''; const histPath=points.map((r,i)=>`${i?'L':'M'}${x(periods.indexOf(r.period))},${y(r.share)}`).join(' '); const projected=[last,...future].map((r,i)=>`${i?'L':'M'}${x(periods.indexOf(r.period))},${y(i?r.flat:last.share)}`).join(' '); return `<path class="chart-line" stroke="${color}" d="${histPath}"/><path class="chart-line projected-line" stroke="${color}" d="${projected}"/>`}).join('');
  const labels=[0,Math.floor((periods.length-1)/2),periods.length-1].map(i=>`<text class="axis-label" x="${x(i)}" y="${H-9}" text-anchor="middle">${safe(periods[i])}</text>`).join('');
  const split=x(periods.indexOf(data.forecast_candidates[0]?.period));
  byId('forecast-chart').innerHTML=`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Thị phần lịch sử và kịch bản giữ nguyên tỷ trọng">${grid}<line class="forecast-divider" x1="${split}" x2="${split}" y1="${T}" y2="${H-B}"/>${paths}${labels}</svg>`;
  byId('forecast-legend').innerHTML=chosen.map(c=>`<span><i style="background:${colors[c.cluster%colors.length]}"></i>${safe(groupName(c.cluster,rows))}</span>`).join('')+'<span class="legend-note">Nét liền: lịch sử · nét đứt: giữ nguyên tỷ trọng gần nhất</span>';
}

function engagementBars(items, rows) {
  const shown=items.filter(r=>r.cluster>=0&&r.median_like_per_view!=null).sort((a,b)=>b.median_like_per_view-a.median_like_per_view).slice(0,7);
  if(!shown.length) return empty('engagement-chart','Chưa có đủ snapshot tương tác để so sánh');
  const max=Math.max(.001,...shown.map(r=>r.median_like_per_view));
  byId('engagement-chart').innerHTML=shown.map(r=>`<div class="signal-bar"><span class="signal-name">${safe(groupName(r.cluster,rows))}</span><span class="signal-track neutral"><i style="width:${Math.max(2,r.median_like_per_view/max*100)}%"></i></span><b>${pct(r.median_like_per_view)}</b></div>`).join('');
}

function graphPositions(nodes, edges) {
  const W=1160,H=860,cx=W/2,cy=H/2,positions=new Map(),n=nodes.length;
  nodes.forEach((node,i)=>{const angle=i*2.3999632297, radius=35+Math.sqrt(i+1)*24;positions.set(node.id,{x:cx+Math.cos(angle)*radius,y:cy+Math.sin(angle)*radius,vx:0,vy:0});});
  const indexById=new Map(nodes.map((node,index)=>[node.id,index]));
  const links=edges.map(edge=>({a:positions.get(edge.source),b:positions.get(edge.target),i:indexById.get(edge.source),j:indexById.get(edge.target),w:edge.weight})).filter(e=>e.a&&e.b);
  const maxWeight=Math.max(1,...links.map(e=>e.w));
  for(let step=0;step<150;step++){
    const fx=Array(n).fill(0),fy=Array(n).fill(0),cool=1-step/150;
    for(let i=0;i<n;i++)for(let j=i+1;j<n;j++){
      const a=positions.get(nodes[i].id),b=positions.get(nodes[j].id),dx=a.x-b.x,dy=a.y-b.y,d2=dx*dx+dy*dy+1,d=Math.sqrt(d2),force=Math.min(18,2200/d2);
      fx[i]+=dx/d*force;fy[i]+=dy/d*force;fx[j]-=dx/d*force;fy[j]-=dy/d*force;
    }
    for(const edge of links){const dx=edge.b.x-edge.a.x,dy=edge.b.y-edge.a.y,d=Math.sqrt(dx*dx+dy*dy)+.01,weight=Math.sqrt(edge.w/maxWeight),force=(d-(150-45*weight))*.010;
      const {i,j}=edge;
      if(i>=0){fx[i]+=dx/d*force;fy[i]+=dy/d*force}if(j>=0){fx[j]-=dx/d*force;fy[j]-=dy/d*force}
    }
    nodes.forEach((node,i)=>{const p=positions.get(node.id);fx[i]+=(cx-p.x)*.0017;fy[i]+=(cy-p.y)*.0017;p.vx=(p.vx+fx[i])*.68;p.vy=(p.vy+fy[i])*.68;p.x=Math.max(38,Math.min(W-38,p.x+p.vx*cool));p.y=Math.max(38,Math.min(H-38,p.y+p.vy*cool));});
  }
  return {W,H,positions};
}

function showLinkDetail(node, edge=null, baseline=0) {
  const detail=byId('network-detail');
  if(edge){detail.innerHTML=`<span class="detail-kind">LI\u00caN K\u1ebeT C\u00d9NG XU\u1ea4T HI\u1ec6N</span><h3>#${safe(edge.source_tag)} \u2194 #${safe(edge.target_tag)}</h3><p>${fmt(edge.shared_posts)} b\u00e0i d\u00f9ng chung \u00b7 ${fmt(edge.shared_creators)} creator \u00b7 L\u1ee3i th\u1ebf ${fmt(edge.lift,2)}\u00d7 \u00b7 Tr\u1ecdng s\u1ed1 PPMI ${fmt(edge.weight,3)}</p>`;return;}
  const multiple=baseline>0?node.articlerank/baseline:0;
  detail.innerHTML=`<span class="detail-kind">HASHTAG \u00b7 \u0110I\u1ec2M TO\u00c0N M\u1ea0NG</span><h3>${safe(node.label)}</h3><p>${fmt(node.posts)} b\u00e0i \u00b7 ${fmt(node.links)} li\u00ean k\u1ebft \u0111\u1ea1t ng\u01b0\u1ee1ng \u00b7 ArticleRank ${fmt(node.articlerank,7)} (${fmt(multiple,1)}\u00d7 m\u1ee9c n\u1ec1n) \u00b7 Closeness Centrality ${fmt(node.closeness,4)}</p>`;
}

function renderRankList(id, rows, metric, baseline=0) {
  const max=Math.max(1e-12,...rows.map(row=>row[metric]||0));
  byId(id).innerHTML=rows.slice(0,12).map((row,index)=>{const raw=row[metric]||0,display=metric==='articlerank'?`${fmt(baseline?raw/baseline:0,1)}\u00d7`:`${fmt(raw,4)}`,title=metric==='articlerank'?`ArticleRank th\u00f4: ${fmt(raw,8)}; m\u1ee9c n\u1ec1n ${fmt(baseline,8)}`:`Closeness: ${fmt(raw,6)}`;return `<button class="rank-item" type="button" data-tag="${safe(row.tag)}"><span class="rank-index">${String(index+1).padStart(2,'0')}</span><span class="rank-tag">#${safe(row.tag)}<small>${fmt(row.posts)} b\u00e0i \u00b7 ${fmt(row.links)} li\u00ean k\u1ebft</small></span><span class="rank-score" title="${title}"><i><b style="width:${Math.max(2,raw/max*100)}%"></b></i><strong>${display}</strong></span></button>`}).join('')||'<div class="empty-chart">K\u1ef3 n\u00e0y kh\u00f4ng c\u00f3 node \u0111\u1ea1t ng\u01b0\u1ee1ng.</div>';
}

function renderLinkAnalysis(data) {
  const s=data.summary,rank=data.rankings||[],closeness_rankings=data.closeness_rankings||[],baseline=data.articlerank_baseline||(.15/Math.max(1,s.hashtags));
  byId('network-summary').textContent=`\u0110i\u1ec3m t\u00ednh tr\u00ean ${fmt(s.hashtags)} hashtag \u00b7 hi\u1ec7n ${fmt(s.displayed_nodes)} node / ${fmt(s.displayed_edges)} c\u1ea1nh`;
  byId('link-kpi-nodes').textContent=fmt(s.hashtags);byId('link-kpi-edges').textContent=fmt(s.scored_pairs);
  byId('link-kpi-central').textContent=s.articlerank_leader?`#${s.articlerank_leader}`:'\u2014';byId('link-kpi-closeness').textContent=s.closeness_leader?`#${s.closeness_leader}`:'\u2014';
  byId('network-chart-note').textContent=`ArticleRank \u0111\u01b0\u1ee3c so v\u1edbi m\u1ee9c n\u1ec1n to\u00e0n m\u1ea1ng (${fmt(baseline,8)}); s\u1ed1 c\u1eb7p b\u1ecb lo\u1ea1i: ${fmt(s.suppressed_pairs)}.`;
  renderRankList('articlerank-table',rank,'articlerank',baseline);renderRankList('closeness-table',closeness_rankings,'closeness');
  const stage=byId('network-stage'),nodes=data.nodes||[],edges=data.edges||[];
  if(!nodes.length){empty('network-stage','Kh\u00f4ng c\u00f3 li\u00ean k\u1ebft hashtag \u0111\u1ee7 ng\u01b0\u1ee1ng trong k\u1ef3 \u0111\u00e3 ch\u1ecdn.');return;}
  const {W,H,positions}=graphPositions(nodes,edges),maxScore=Math.max(1e-12,...nodes.map(n=>n.score||0)),maxWeight=Math.max(1e-12,...edges.map(e=>e.weight||0)),maxCloseness=Math.max(1e-12,...nodes.map(n=>n.closeness||0));
  const edgeSvg=edges.map(edge=>{const a=positions.get(edge.source),b=positions.get(edge.target),opacity=.16+.7*Math.sqrt(edge.weight/maxWeight),width=.6+2.6*Math.sqrt(edge.weight/maxWeight);return `<line class="link-edge" tabindex="0" role="button" data-edge="${safe(edge.source)}|${safe(edge.target)}" data-source="${safe(edge.source)}" data-target="${safe(edge.target)}" x1="${a.x.toFixed(1)}" y1="${a.y.toFixed(1)}" x2="${b.x.toFixed(1)}" y2="${b.y.toFixed(1)}" stroke-width="${width.toFixed(2)}" opacity="${opacity.toFixed(2)}"><title>#${safe(edge.source_tag)} \u2194 #${safe(edge.target_tag)} \u00b7 ${fmt(edge.shared_posts)} b\u00e0i d\u00f9ng chung \u00b7 lift ${fmt(edge.lift,2)}\u00d7</title></line>`}).join('');
  const nodeSvg=nodes.map(node=>{const p=positions.get(node.id),r=10+12*Math.sqrt((node.score||0)/maxScore),isClose=node.closeness>=maxCloseness*.75;return `<g class="link-node ${isClose?'is-closeness':''}" tabindex="0" role="button" data-node="${safe(node.id)}" aria-label="${safe(node.label)}; ArticleRank ${fmt(node.articlerank,7)}; Closeness: ${fmt(node.closeness,4)}"><title>${safe(node.label)} \u00b7 ${fmt(node.posts)} b\u00e0i \u00b7 ArticleRank ${fmt(node.articlerank,7)} (${fmt(baseline?node.articlerank/baseline:0,1)}\u00d7 m\u1ee9c n\u1ec1n) \u00b7 Closeness: ${fmt(node.closeness,4)}</title><circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="${r.toFixed(1)}"/><text x="${p.x.toFixed(1)}" y="${(p.y+r+13).toFixed(1)}" text-anchor="middle">${safe(node.label.length>16?node.label.slice(0,15)+'\u2026':node.label)}</text></g>`}).join('');
  stage.innerHTML=`<svg viewBox="0 0 ${W} ${H}" aria-label="M\u1ea1ng hashtag x\u1ebfp h\u1ea1ng theo ${safe(data.rank_by)}"><g id="network-layer">${edgeSvg}${nodeSvg}</g></svg>`;
  const svg=stage.querySelector('svg'),layer=stage.querySelector('#network-layer'),nodeById=new Map(nodes.map(n=>[n.id,n])),edgeByKey=new Map(edges.map(e=>[`${e.source}|${e.target}`,e])),original=new Map([...positions].map(([id,p])=>[id,{...p}]));
  const screenPoint=e=>{const pt=svg.createSVGPoint();pt.x=e.clientX;pt.y=e.clientY;return pt.matrixTransform(svg.getScreenCTM().inverse())};
  const update=()=>{for(const el of stage.querySelectorAll('.link-node')){const p=positions.get(el.dataset.node);if(!p)continue;const c=el.querySelector('circle'),t=el.querySelector('text'),r=Number(c.getAttribute('r'));c.setAttribute('cx',p.x);c.setAttribute('cy',p.y);t.setAttribute('x',p.x);t.setAttribute('y',p.y+r+13)}for(const el of stage.querySelectorAll('.link-edge')){const a=positions.get(el.dataset.source),b=positions.get(el.dataset.target);if(a&&b){el.setAttribute('x1',a.x);el.setAttribute('y1',a.y);el.setAttribute('x2',b.x);el.setAttribute('y2',b.y)}}};
  let gesture=null;
  svg.addEventListener('pointerdown',e=>{const nodeEl=e.target.closest('.link-node');if(nodeEl){gesture={kind:'node',id:nodeEl.dataset.node,el:nodeEl,start:screenPoint(e),origin:{...positions.get(nodeEl.dataset.node)},moved:false};nodeEl.classList.add('dragging')}else if(e.target===svg||e.target===layer){gesture={kind:'pan',startX:e.clientX,startY:e.clientY,view:{...svg.viewBox.baseVal}};stage.classList.add('is-panning')}else return;svg.setPointerCapture(e.pointerId);e.preventDefault()});
  svg.addEventListener('pointermove',e=>{if(!gesture)return;if(gesture.kind==='node'){const p=screenPoint(e),dx=p.x-gesture.start.x,dy=p.y-gesture.start.y;if(Math.abs(dx)+Math.abs(dy)>2)gesture.moved=true;positions.set(gesture.id,{...gesture.origin,x:gesture.origin.x+dx,y:gesture.origin.y+dy});update()}else{const rect=svg.getBoundingClientRect(),scale=gesture.view.width/rect.width;svg.setAttribute('viewBox',`${gesture.view.x-(e.clientX-gesture.startX)*scale} ${gesture.view.y-(e.clientY-gesture.startY)*scale} ${gesture.view.width} ${gesture.view.height}`)}});
  const stop=e=>{if(!gesture)return;if(gesture.kind==='node'){gesture.el.classList.remove('dragging');if(gesture.moved){gesture.el.dataset.dragged='1';setTimeout(()=>delete gesture.el.dataset.dragged,0)}}gesture=null;stage.classList.remove('is-panning')};svg.addEventListener('pointerup',stop);svg.addEventListener('pointercancel',stop);
  svg.addEventListener('click',e=>{if(e.target.closest('.link-node,.link-edge'))return;if(!e.target.closest('#network-layer')){stage.querySelectorAll('.selected').forEach(x=>x.classList.remove('selected'))}});
  stage.querySelectorAll('.link-node').forEach(el=>{const activate=()=>{if(el.dataset.dragged)return;stage.querySelectorAll('.selected').forEach(x=>x.classList.remove('selected'));el.classList.add('selected');const node=nodeById.get(el.dataset.node);if(node)showLinkDetail(node,null,baseline)};el.addEventListener('click',activate);el.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();activate()}})});
  stage.querySelectorAll('.link-edge').forEach(el=>{const activate=()=>{stage.querySelectorAll('.selected').forEach(x=>x.classList.remove('selected'));el.classList.add('selected');const edge=edgeByKey.get(el.dataset.edge);if(edge)showLinkDetail(null,edge,baseline)};el.addEventListener('click',activate);el.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();activate()}})});
  const zoom=factor=>{const vb=svg.viewBox.baseVal,cx=vb.x+vb.width/2,cy=vb.y+vb.height/2,w=vb.width*factor,h=vb.height*factor;svg.setAttribute('viewBox',`${cx-w/2} ${cy-h/2} ${w} ${h}`)};
  svg.addEventListener('wheel',e=>{e.preventDefault();zoom(e.deltaY<0?.88:1.14)},{passive:false});byId('network-zoom-in').onclick=()=>zoom(.8);byId('network-zoom-out').onclick=()=>zoom(1.25);byId('network-reset').onclick=()=>{for(const [id,p] of original)positions.set(id,{...p});svg.setAttribute('viewBox',`0 0 ${W} ${H}`);update();stage.querySelectorAll('.selected').forEach(x=>x.classList.remove('selected'))};
  const first=rank[0];if(first)showLinkDetail(nodes.find(n=>n.tag===first.tag)||nodes[0],null,baseline);
  document.querySelectorAll('.rank-item').forEach(button=>button.addEventListener('click',()=>{const node=nodes.find(n=>n.tag===button.dataset.tag);if(node)showLinkDetail(node,null,baseline)}));
}

let loadedNetworkPeriod='';
let networkRequest=0;
async function loadNetwork(force=false){
  const start=byId('from').value,end=byId('to').value;
  if(!start||!end||start>end)return;
  const period=`${start}|${end}`;
  if(!force&&loadedNetworkPeriod===period)return;
  const requestId=++networkRequest;
  const q=new URLSearchParams({from:start,to:end,rank_by:byId('network-rank').value,display_limit:byId('network-limit').value});
  byId('network-summary').textContent='Đang dựng mạng hashtag và tính ArticleRank / Closeness Centrality…';
  try{const response=await fetch(`/api/graph-analysis?${q}`);if(!response.ok)throw new Error('Không chạy được link analysis trên graph.');renderLinkAnalysis(await response.json());}
  catch(error){byId('network-summary').textContent=error.message;empty('network-stage','Không thể tính điểm mạng lưới.');}
}

function freshness(data) {
  const date=data.source_end, start=byId('from').value, end=byId('to').value;
  const display=(value)=>value?new Intl.DateTimeFormat('vi-VN',{day:'2-digit',month:'2-digit',year:'numeric',timeZone:'UTC'}).format(new Date(`${value}T00:00:00Z`)):'—';
  byId('freshness').innerHTML=`<b>Kỳ báo cáo:</b> ${display(start)} – ${display(end)} <span class="scope-separator">·</span> <b>Dữ liệu đến:</b> ${safe(date)} <span class="scope-separator">·</span> Mẫu creator ViTIED`;
  byId('source-date').textContent=`Dữ liệu đến ${display(date)}`;
}

function render(data) {
  const trend=data.future_trends, rows=trend.momentum||[]; freshness(trend);
  changeBars(rows); breadthPlot(rows); forecastChart(trend,rows); engagementBars(data.cluster_engagement||[],rows);
  byId('change-chart-detail').innerHTML=byId('change-chart').innerHTML;
  byId('breadth-chart-detail').innerHTML=byId('breadth-chart').innerHTML;
  const positive=rows.filter(r=>r.cluster>=0&&r.share_change_pp>0).sort((a,b)=>b.share_change_pp-a.share_change_pp);
  const leader=positive[0];
  if(leader){byId('headline').textContent=`${groupName(leader.cluster,rows)} tăng tỷ trọng mạnh nhất trong kỳ`;byId('headline-delta').textContent=`+${fmt(leader.share_change_pp,1)} điểm %`;byId('headline-support').textContent=`${fmt(leader.creators_recent)} creator trong 4 tuần cuối`;byId('headline-copy').textContent=`Ghi nhận tăng ở ${leader.weeks_share_up} trên 4 tuần gần nhất.`}
  else {byId('headline').textContent='Chưa thấy nhóm nội dung tăng rõ trong mẫu';byId('headline-delta').textContent='—';byId('headline-support').textContent='Không có nhóm tăng thị phần';}
  byId('signal-rows').innerHTML=rows.map(r=>`<tr><td><b>${safe(groupName(r.cluster,rows))}</b><small class="term-hint">${safe((r.caption_terms||[]).slice(0,3).join(' · ')||'nhóm chưa gán')}</small></td><td class="numeric">${pct(r.share_previous)}</td><td class="numeric">${pct(r.share_recent)}</td><td class="numeric ${r.share_change_pp>=0?'gain-text':'loss-text'}">${r.share_change_pp>0?'+':''}${fmt(r.share_change_pp,1)} đpt</td><td class="numeric">${fmt(r.creators_recent)}</td><td class="numeric">${r.weeks_share_up} / 4</td><td class="numeric">${pct(r.forecast_share)}</td></tr>`).join('');
  byId('updated').textContent=`Cập nhật giao diện ${new Intl.DateTimeFormat('vi-VN',{hour:'2-digit',minute:'2-digit'}).format(new Date())}`;
}

const viewDetails={
  overview:['Tổng quan xu hướng','Phân tích nhịp nội dung, độ phủ creator và phản hồi trong kỳ.'],
  momentum:['Nhịp tăng / giảm','So sánh thay đổi tỷ trọng và độ phủ creator giữa các nhóm.'],
  projection:['Kịch bản 4 tuần','Thị phần tham chiếu theo tỷ trọng ghi nhận gần nhất.'],
  engagement:['Phản hồi nội dung','So sánh tỷ lệ lượt thích trên lượt xem giữa các nhóm.'],
  groups:['So sánh nhóm','Đối chiếu tỷ trọng, creator và thay đổi theo nhóm nội dung.'],
  network:['M\u1ea1ng hashtag to\u00e0n c\u1ee5c','X\u1ebfp h\u1ea1ng vai tr\u00f2 trung t\u00e2m v\u00e0 hashtag c\u1ea7u n\u1ed1i b\u1eb1ng link analysis.'],
};
function showView(name, updateHash=true){
  const chosen=viewDetails[name]?name:'overview';
  document.querySelectorAll('.report-view').forEach(el=>el.classList.toggle('active',el.dataset.report===chosen));
  document.querySelectorAll('.report-link').forEach(el=>el.classList.toggle('active',el.dataset.view===chosen));
  byId('view-title').textContent=viewDetails[chosen][0]; byId('view-subtitle').textContent=viewDetails[chosen][1];
  if(updateHash&&location.hash!==`#${chosen}`) history.replaceState(null,'',`#${chosen}`);
  window.scrollTo({top:0,behavior:'smooth'});
  if(chosen==='network') loadNetwork();
}
document.querySelectorAll('.report-link').forEach(link=>link.addEventListener('click',event=>{event.preventDefault();showView(link.dataset.view)}));
document.querySelectorAll('[data-go]').forEach(button=>button.addEventListener('click',()=>showView(button.dataset.go)));
window.addEventListener('hashchange',()=>showView(location.hash.slice(1),false));
byId('network-load').addEventListener('click',loadNetwork);
byId('network-rank').addEventListener('change',()=>loadNetwork(true));
byId('network-limit').addEventListener('change',()=>loadNetwork(true));
showView(location.hash.slice(1)||'overview',false);

async function loadMeta() {
  const res=await fetch('/api/meta'); if(!res.ok) throw new Error('Không kết nối được nguồn phân tích.');
  const meta=await res.json(), min=meta.period?.min_date, max=meta.period?.max_date;
  byId('from').value=min||''; byId('to').value=max||''; byId('from').min=min||''; byId('to').max=max||'';
}
async function loadDashboard() {
  const error=byId('error'); error.hidden=true;
  const from=byId('from').value,to=byId('to').value;
  if(!from||!to||from>to){error.textContent='Khoảng ngày không hợp lệ.';error.hidden=false;return;}
  try {const q=new URLSearchParams({from,to,media_type:'all'}),res=await fetch(`/api/dashboard?${q}`);if(!res.ok)throw new Error('Không tải được phân tích. Hãy thử làm mới.');render(await res.json());}
  catch(e){error.textContent=e.message;error.hidden=false;}
}
const reloadReports=async()=>{await loadDashboard();loadedNetworkPeriod='';if(document.querySelector('[data-report="network"]').classList.contains('active'))await loadNetwork(true);};
byId('apply').addEventListener('click',reloadReports);byId('refresh').addEventListener('click',reloadReports);
byId('from').addEventListener('change',reloadReports);byId('to').addEventListener('change',reloadReports);
loadMeta().then(reloadReports).catch(e=>{byId('error').textContent=e.message;byId('error').hidden=false;});
