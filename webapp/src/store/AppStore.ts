import { observable, flow } from "mobx";
import axios from "axios";
import { message } from "antd";

export type AdapterType = {
  name: string,
  func_name: string,
  description: string,
  input: { [key: string]: any; },
  ouput: { [key: string]: any; }
};

export type PipelineType = {
  name: string,
  description: string,
  start_timestamp: string,
  status: string,
  end_timestamp: string,
  config_file: string,
  id: string,
};

export class AppStore {
  @observable isInited: boolean = false;
  @observable entityTypes: string[] = [];
  @observable a: number = 0;
  @observable adapters: AdapterType[] = [];
  @observable pipelines: PipelineType[] = [];
  @observable currentPipeline: PipelineType | null = null;

  /**
   * init the app with data from server
   */
  init = flow(function*(this: AppStore) {
    try {
      const resp: any = yield axios.get(`/`);
      this.entityTypes = resp.data.entity_types;
      this.isInited = true;
      this.getAdapters();
      this.getPipelines();
    } catch (error) {
      message.error(
        `error while initializing the app: ${JSON.stringify(
          error.response.data
        )}`
      );
      throw error;
    }
  });

  // FIXME: this is not the best way to do call backend API
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

  getAdapters = () => {
    const resp: any = axios.get(`/adapters`).then(
      (resp) => {
        if ("data" in resp) {
          this.adapters = resp.data;
        } else {
          console.log("THERE IS SOMETHING WRONG!");
        }
      }
    );
  }

  getPipelines = () => {
    const resp: any = axios.get(`/pipelines`).then(
      (resp) => {
        if ("data" in resp) {
          this.pipelines = resp.data;
        } else {
          console.log("THERE IS SOMETHING WRONG!");
        }
      }
    );
  }

  getPipeline = (pipelineId: string) => {
    const resp: any = axios.get(`/pipelines/${pipelineId}`).then(
      (resp) => {
        if ("data" in resp) {
          this.currentPipeline = resp.data;
        } else {
          console.log("THERE IS SOMETHING WRONG!");
        }
      }
    );
  }
}
