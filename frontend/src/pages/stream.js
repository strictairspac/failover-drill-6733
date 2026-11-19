import {store} from '../stores/entities.js';
import {session} from '../stores/session.js';
import {streamsAPI} from '../api/entities.js';
import {table} from '../components/common/table.js';
import {esc,formObject} from '../utils/html.js';
import {action,message} from '../hooks/action.js';
import {STREAM_PHASES,RECEIPT_KINDS,STREAM_EVENTS} from '../constants/enums.js';

const pill=t=>`<span class="pill">${esc(t)}</span>`;
const chip=(c,cls='')=>`<span class="chip ${cls}">${esc(c.id)} · ${esc(c.order)}<small>${esc(c.text||'')}</small></span>`;
const writable=()=>session.role!=='viewer';

export function streamsPage(){
 const followers=store.nodes.filter(n=>n.role==='follower');
 return `<section class="split compact"><div><h2>建立一条续接演练流</h2><p>声明接任副本与订阅方后，主节点开始发出订单变更。复制可随时中断，切主沿用原选主规则（多数派 · 隔离 · RPO · 任期）。</p></div>${writable()?`<form id="stream-form"><label>演练名称<input name="name" value="订单流续接演练" required maxlength="100"></label><label>故障剧本<select name="scenario_id">${store.scenarios.map(s=>`<option value="${s.id}">${esc(s.name)}</option>`).join('')}</select></label><label>接任副本<select name="replica_name">${followers.map(n=>`<option value="${esc(n.name)}">${esc(n.name)}</option>`).join('')}</select></label><label>订阅方（逗号分隔，至多6个）<input name="subscribers" value="对账中心,通知服务,风控平台" required></label><button type="submit">冻结拓扑并开流 →</button></form>`:'<p class="readonly">观察员可复核下方演练流。</p>'}</section><div class="section-title"><h2>续接演练流</h2><button class="secondary" id="refresh">刷新</button></div>${table(['编号','名称','阶段','任期','主 → 副本','变更数','订阅方','待收回执',''],store.streams.map(s=>[s.id,esc(s.name),pill(STREAM_PHASES[s.phase]||s.phase),s.term??'—',esc(s.primary_name||'')+' → '+esc(s.replica_name||''),s.changes,s.subscribers,s.pending_receipts,`<a href="#stream/${s.id}">进入沙盘 →</a>`]))}`;
}
export function bindStreams(render){
 document.querySelector('#refresh').onclick=render;
 action(document.querySelector('#stream-form'),async f=>{const d=formObject(f);const s=await streamsAPI.create({name:d.name,scenario_id:Number(d.scenario_id),replica_name:d.replica_name,subscribers:d.subscribers.split(/[,，]/).map(x=>x.trim()).filter(Boolean)});location.hash='#stream/'+s.id;});
}

function subCard(st,sub){
 const applied=sub.applied.map(c=>chip(c,sub.retracted.includes(c.id)?'retracted':'')).join('')||'<span class="muted">尚无</span>';
 const waiting=[...sub.retract_queue.map(c=>chip(c,'retract-wait')),...st.log.slice(sub.next_lsn-1).map(c=>chip(c,'wait'))];
 return `<div class="subcard"><div class="subhead"><b>${esc(sub.name)}</b>${writable()?`<label class="check"><input type="checkbox" class="pause" data-name="${esc(sub.name)}" ${sub.paused?'checked':''}> 暂停接收</label>`:(sub.paused?pill('已暂停'):'')}</div>
 <p class="muted">续送游标 LSN ${sub.next_lsn}${sub.paused?' · 已暂停，记录在积压':''}</p>
 <div class="chiprow"><span class="lbl">已应用</span>${applied}</div>
 <div class="chiprow"><span class="lbl">待确认</span>${sub.awaiting?chip(sub.awaiting.change,'awaiting'):'<span class="muted">无</span>'}</div>
 <div class="chiprow"><span class="lbl">在等</span>${waiting.join('')||'<span class="muted">无待送记录</span>'}</div></div>`;
}

export function streamBoardPage(s){
 const st=s.state,el=st.last_election;
 const statusLine=st.phase==='streaming'
  ?`当前任期 ${st.term} · 日志 ${st.log.length} 条 · 副本已复制 ${st.replica_position} 条 · 复制${st.replication_on?'正常':'中断中'}`
  :`当前任期 ${st.term} · 新主 ${esc(st.replica_name)} · 新日志 ${st.log.length} 条 · 分叉隔离 ${st.forked.length} 条`;
 const replicaMark=c=>st.phase==='streaming'?(c.lsn<=st.replica_position?'已入副本':'未到达副本'):(c.term===st.term?'新主变更':'共有前缀');
 return `<div class="section-title"><h2>${esc(s.name)} <small>#${s.id}</small></h2>${pill(STREAM_PHASES[s.phase]||s.phase)}</div>
<p class="muted">主 ${esc(st.primary_name)} → 接任副本 ${esc(st.replica_name)} · ${statusLine}</p>
${el?`<section class="verdict"><div><p>切主规则复核</p><h2>${el.decision==='approved'?'允许切主':'安全阻断'}</h2></div><p>${el.winner?'当选 '+esc(el.winner):'无合格候选'}<br>新任期 ${el.new_term??'—'} · 多数派 ${el.reachable_votes??'—'}/${el.quorum??'—'}</p></section><details><summary>查看切主规则逐项判定</summary>${table(['规则','判定','输入证据'],(el.checks||[]).map(c=>[esc(c.rule),c.pass?'通过':'阻断',esc(c.evidence)]))}</details>`:''}
${writable()?`<div class="toolbar wrap"><form id="emit-form" class="inline"><label>订单号<input name="order" value="ORD-1001" required maxlength="60"></label><label>变更内容<input name="text" value="状态推进" required maxlength="120"></label><button type="submit">发出变更</button></form><button id="deliver">投递一轮</button><button id="collect">收取回执（${st.receipts.length}）</button>${st.phase==='streaming'?`<button class="secondary" id="toggle-repl">${st.replication_on?'中断复制':'恢复复制'}</button><button class="danger" id="failover">执行切主 →</button>`:''}<button class="secondary" id="refresh">刷新</button></div>`:`<div class="toolbar"><button class="secondary" id="refresh">刷新</button></div>`}
<h2>主日志</h2>${table(['LSN','变更号','订单','内容','任期','复制状态'],st.log.map(c=>[c.lsn,esc(c.id),esc(c.order),esc(c.text),'任期 '+c.term,replicaMark(c)]))}
${st.forked&&st.forked.length?`<h2>分叉隔离区（旧主发出、未到达新主）</h2>${table(['变更号','订单','内容','任期','处置'],st.forked.map(c=>[esc(c.id),esc(c.order),esc(c.text),'任期 '+c.term,'已隔离 · 按订阅方逐一撤回']))}`:''}
<div class="section-title"><h2>订阅方</h2><span>${st.subscribers.length} 个 · 各自游标独立续送</span></div><div class="subgrid">${st.subscribers.map(sub=>subCard(st,sub)).join('')}</div>
<div class="section-title"><h2>回执通道</h2><span>收取后确认；标记丢失可演练重试去重</span></div>${table(['回执','订阅方','变更','类型','任期','操作'],st.receipts.map(r=>[esc(r.id),esc(r.subscriber),esc(r.change_id),RECEIPT_KINDS[r.kind]||r.kind,'任期 '+r.term,writable()?`<button class="secondary small drop" data-rid="${esc(r.id)}">标记丢失</button>`:'']))}
<div class="section-title"><h2>演练时间线</h2><span>${st.timeline.length} 条事件</span></div>${table(['#','事件'],[...st.timeline].reverse().map(e=>[e.id,`<span class="kind ${esc(e.kind)}">${esc(STREAM_EVENTS[e.kind]||e.kind)}</span> ${esc(e.text)}`]))}`;
}
export function bindStreamBoard(s,render){
 const run=async p=>{try{await p;}catch(e){message(e.message,true);}render();};
 document.querySelector('#refresh')?.addEventListener('click',render);
 document.querySelector('#deliver')?.addEventListener('click',()=>run(streamsAPI.act(s.id,'deliver')));
 document.querySelector('#collect')?.addEventListener('click',()=>run(streamsAPI.act(s.id,'receipts/collect')));
 document.querySelector('#toggle-repl')?.addEventListener('click',()=>run(streamsAPI.act(s.id,'replication',{on:!s.state.replication_on})));
 document.querySelector('#failover')?.addEventListener('click',()=>{if(confirm('按原切主规则（多数派 · 隔离 · RPO · 任期）执行切主？'))run(streamsAPI.act(s.id,'failover'));});
 document.querySelectorAll('.drop').forEach(b=>b.addEventListener('click',()=>run(streamsAPI.drop(s.id,b.dataset.rid))));
 document.querySelectorAll('.pause').forEach(c=>c.addEventListener('change',()=>run(streamsAPI.pause(s.id,c.dataset.name,c.checked))));
 const f=document.querySelector('#emit-form');
 if(f)f.addEventListener('submit',e=>{e.preventDefault();const d=formObject(f);run(streamsAPI.act(s.id,'changes',{order:d.order,text:d.text}));});
}
