import {session} from './stores/session.js';
import {store} from './stores/entities.js';
import {drillsAPI,streamsAPI} from './api/entities.js';
import {layout} from './components/common/layout.js';
import {loginPage,bindLogin} from './pages/login.js';
import {nodesPage,bindNodes} from './pages/nodes.js';
import {scenariosPage,bindScenarios} from './pages/scenarios.js';
import {drillsPage,bindDrills} from './pages/drills.js';
import {detailPage,bindDetail} from './pages/detail.js';
import {streamsPage,bindStreams,streamBoardPage,bindStreamBoard} from './pages/stream.js';
import {routes} from './router/routes.js';
import {esc} from './utils/html.js';
const app=document.querySelector('#app');
async function render(){
 const [key,id]=location.hash.slice(1).split('/');
 if(!session.token||key==='login'){app.innerHTML=loginPage();bindLogin();return;}
 try{await store.refresh();let body,job,stream;const route=routes[key]?key:'nodes';
 if(route==='nodes')body=nodesPage();if(route==='scenarios')body=scenariosPage();if(route==='drills')body=drillsPage();if(route==='detail'){job=id?await drillsAPI.get(Number(id)):store.drills[0];body=detailPage(job);}
 if(route==='stream'){stream=id?await streamsAPI.get(Number(id)):null;body=stream?streamBoardPage(stream):streamsPage();}
 app.innerHTML=layout(...routes[route],body);document.querySelector('#logout').onclick=()=>{session.clear();location.hash='#login';};
 if(route==='nodes')bindNodes(render);if(route==='scenarios')bindScenarios(render);if(route==='drills')bindDrills(render);if(route==='detail')bindDetail(job,render);
 if(route==='stream'){stream?bindStreamBoard(stream,render):bindStreams(render);}
 }catch(e){app.innerHTML=layout('暂时无法载入','请检查服务状态后重试。',`<p class="notice error">${esc(e.message)}</p><button id="retry">重新加载</button>`);document.querySelector('#retry').onclick=render;document.querySelector('#logout').onclick=()=>{session.clear();location.hash='#login';};}
}
window.addEventListener('hashchange',render);render();
