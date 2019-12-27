import { createBrowserHistory } from "history";
import { RouterStore, syncHistoryWithStore } from "mobx-react-router";
import { PipelineStore } from "./PipelineStore";
import { AdapterStore } from "./AdapterStore";
// import { autorun } from "mobx";

export const routingStore = new RouterStore();
export const stores = {
  routing: routingStore,
  pipelineStore: new PipelineStore(),
  adapterStore: new AdapterStore(),
};

// autorun(() => {
//   console.log("HERE is the uploadedPipeline!");
//   console.log(stores.app.uploadedPipeline);
// })


export type IStore = Readonly<typeof stores>;
export const history = syncHistoryWithStore(
  createBrowserHistory(),
  routingStore
);
