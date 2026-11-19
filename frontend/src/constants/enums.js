export const ROLES = {admin:'管理员',operator:'操作员',viewer:'观察员'};
export const DRILL_STATUSES = {queued:'等待执行',running:'执行中',completed:'已完成',failed:'失败'};
export const STREAM_PHASES = {streaming:'流式复制中',promoted:'已切主续接'};
export const RECEIPT_KINDS = {change:'应用',retract:'撤回'};
export const STREAM_EVENTS = {init:'建立',emit:'变更',replicate:'复制',pause:'暂停',deliver:'投递',retry:'重试',dedupe:'去重',retract:'撤回',receipt:'回执',collect:'确认',late:'迟到',lost:'丢失',failover:'切主',blocked:'阻断',handshake:'握手'};
