import {api} from './client.js';
export const nodesAPI={list:()=>api('/nodes'),save:(data,id)=>api('/nodes'+(id?'/'+id:''),{method:id?'PUT':'POST',body:JSON.stringify(data)})};
export const scenariosAPI={list:()=>api('/scenarios'),create:data=>api('/scenarios',{method:'POST',body:JSON.stringify(data)})};
export const drillsAPI={list:()=>api('/drills'),get:id=>api('/drills/'+id),create:data=>api('/drills',{method:'POST',body:JSON.stringify(data)})};
export const streamsAPI={list:()=>api('/streams'),get:id=>api('/streams/'+id),create:data=>api('/streams',{method:'POST',body:JSON.stringify(data)}),act:(id,op,data)=>api(`/streams/${id}/${op}`,{method:'POST',body:JSON.stringify(data||{})}),pause:(id,name,paused)=>api(`/streams/${id}/subscribers/${encodeURIComponent(name)}/pause`,{method:'POST',body:JSON.stringify({paused})}),drop:(id,rid)=>api(`/streams/${id}/receipts/${encodeURIComponent(rid)}/drop`,{method:'POST',body:JSON.stringify({})})};
