import { observable, flow, action } from "mobx";
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
  adapters?: AdapterType[]
};

export class AppStore {
  @observable isInited: boolean = false;
  @observable entityTypes: string[] = [];
  @observable adapters: AdapterType[] = [];
  @observable pipelines: PipelineType[] = [];
  @observable currentPipeline: PipelineType | null = null;
  @observable uploadedPipeline: PipelineType | null = null;

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

  @action getAdapters = () => {
    axios.get(`/adapters`).then(
      (resp) => {
        if ("data" in resp) {
          this.adapters = resp.data;
        } else {
          console.log("THERE IS SOMETHING WRONG!");
        }
      }
    );
  }

  @action getPipelines = () => {
    axios.get(`/pipelines`).then(
      (resp) => {
        if ("data" in resp) {
          this.pipelines = resp.data;
        } else {
          console.log("THERE IS SOMETHING WRONG!");
        }
      }
    );
  }

  @action getPipeline = (pipelineId: string) => {
    axios.get(`/pipelines/${pipelineId}`).then(
      (resp) => {
        if ("data" in resp) {
          this.currentPipeline = resp.data;
        } else {
          console.log("THERE IS SOMETHING WRONG!");
        }
      }
    );
  }

  @action setUploadedPipeline = (uploadedPipeline: PipelineType | null) => {
    this.uploadedPipeline = uploadedPipeline;
  }
}
