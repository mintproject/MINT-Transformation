import { observable, flow } from "mobx";
import axios from "axios";
import { message } from "antd";

export class AppStore {
  @observable isInited: boolean = false;
  @observable entityTypes: string[] = [];
  @observable a: number = 0;

  /**
   * init the app with data from server
   */
  init = flow(function*(this: AppStore) {
    try {
      const resp: any = yield axios.get(`/`);
      this.entityTypes = resp.data.entity_types;
      this.isInited = true;
      this.a = 22;
    } catch (error) {
      message.error(
        `error while initializing the app: ${JSON.stringify(
          error.response.data
        )}`
      );
      throw error;
    }
  });

  setA = () => {
    const resp: any = axios.get(`/test`).then(
      (resp) => {
        if ("data" in resp && "a" in resp.data) {
          this.a = resp.data.a;
        } else {
          console.log("THERE IS NO A");
        }
      }
    );
  }
}
