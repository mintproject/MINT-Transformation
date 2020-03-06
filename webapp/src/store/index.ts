import {createBrowserHistory} from "history";
import {RouterStore, syncHistoryWithStore} from "mobx-react-router";
import {PipelineStore} from "./PipelineStore";
import {AdapterStore} from "./AdapterStore";
import {ErrorStore} from "./ErrorStore";
// import { autorun } from "mobx";

export const routingStore = new RouterStore();
const errorStore = new ErrorStore();
export const stores = {
  routing: routingStore,
  pipelineStore: new PipelineStore(errorStore),
  adapterStore: new AdapterStore(errorStore),
  errorStore: errorStore
}
// autorun(() => {
//   console.log("HERE is the uploadedPipeline!");
//   console.log(stores.app.uploadedPipeline);
// })


export type IStore = Readonly<typeof stores>;
export const history = syncHistoryWithStore(
  createBrowserHistory(),
  routingStore
);
