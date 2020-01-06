import { observable, action } from "mobx";
import axios from "axios";
import { AdapterType, flaskUrl } from "./AdapterStore";

// FIXME: settle down on the final format of pipeline object:
// metadata + list of adapters?
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

export class PipelineStore {
  @observable pipelines: PipelineType[] = [];
  @observable currentPipeline: PipelineType | null = null;
  @observable uploadedPipeline: object | null = null;
  @observable uploadedPipelineConfig: object | null = null;

  @action.bound getPipelines = () => {
    axios.get(`${flaskUrl}/pipelines`).then(
      (resp) => {
        if ("data" in resp) {
          this.pipelines = resp.data;
        } else {
          console.log("THERE IS SOMETHING WRONG!");
        }
      }
    );
  }

  @action.bound getPipeline = (pipelineId: string) => {
    axios.get(`${flaskUrl}/pipelines/${pipelineId}`).then(
      (resp) => {
        if ("data" in resp) {
          this.currentPipeline = resp.data;
        } else {
          console.log("THERE IS SOMETHING WRONG!");
        }
      }
    );
  }

  @action.bound setUploadedPipeline = (uploadedPipeline?: object | null, dcatId?: string) => {
    if (dcatId !== undefined) {
      axios.get(`${flaskUrl}/pipeline/dcat/${dcatId}`).then(
        (resp) => {
          if ("data" in resp) {
            this.uploadedPipeline = resp.data;
          } else {
            console.log("THERE IS SOMETHING WRONG!");
          }
        }
      );
    } else if (uploadedPipeline !== undefined) {
      console.log("IM HERE IN set upload");
      this.uploadedPipeline = uploadedPipeline;
    }
  }

  @action.bound setUploadedPipelineConfig = (uploadedPipelineConfig: object | null) => {
    console.log("setting uploaded config");
    console.log(uploadedPipelineConfig)
    this.uploadedPipelineConfig = uploadedPipelineConfig;
  }

  @action.bound createPipeline = (pipelineName: string, pipelineDescription: string, pipelineConfig: object) => {
    console.log("IM HERE INSIDE CREATE");
    console.log(pipelineConfig)
    axios.post(`${flaskUrl}/pipeline/create`, {
      name: pipelineName,
      description: pipelineDescription,
      config: pipelineConfig
    }).then(
      (resp) => {
        if ("result" in resp) {
          console.log("HAHAHA");
        }
      }
    );
  }
};


