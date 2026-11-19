import {nodesAPI,scenariosAPI,drillsAPI,streamsAPI} from '../api/entities.js';
export const store={nodes:[],scenarios:[],drills:[],streams:[],async refresh(){[this.nodes,this.scenarios,this.drills,this.streams]=await Promise.all([nodesAPI.list(),scenariosAPI.list(),drillsAPI.list(),streamsAPI.list()]);}};
