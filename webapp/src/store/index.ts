import { createBrowserHistory } from "history";
import { RouterStore, syncHistoryWithStore } from "mobx-react-router";
import { AppStore } from "./AppStore";

export const routingStore = new RouterStore();
export const stores = {
  routing: routingStore,
  app: new AppStore()
};

export type IStore = Readonly<typeof stores>;
export const history = syncHistoryWithStore(
  createBrowserHistory(),
  routingStore
);

export { AppStore };
