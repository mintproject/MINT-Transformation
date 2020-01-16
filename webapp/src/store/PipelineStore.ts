import { observable, action } from "mobx";
import axios from "axios";
import { AdapterType, flaskUrl, AdapterInputType } from "./AdapterStore";
import { message } from "antd";

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

export type UploadedPipelineDataType = {
  nodes: NodeType[],
  edges: EdgeType[]
}

export type NodeType = {
  id: number,
  adapter: string,
  // comment: string
  inputs: AdapterInputType[],
  outputs: AdapterInputType[],
  comment?: string
}

export type EdgeType = {
  source: string,
  target: string,
  input: string,
  output: string,
}

export class PipelineStore {
  @observable pipelines: PipelineType[] = [];
  @observable currentPipeline: PipelineType | null = null;
  @observable uploadedPipelineData: UploadedPipelineDataType | null = null;
  @observable graphCreated: boolean = false;

  @action.bound getPipelines = () => {
    axios.get(`${flaskUrl}/pipelines`).then(
      (resp) => {
        if (resp.data.error) {
          message.info(`An error has occurred: ${resp.data.error}`)
        } else {
          this.pipelines = resp.data;
        }
      }
    );
  }

  @action.bound getPipeline = (pipelineId: string) => {
    axios.get(`${flaskUrl}/pipelines/${pipelineId}`).then(
      (resp) => {
        if (resp.data.error) {
          message.info(`An error has occurred: ${resp.data.error}`);
        } else {
          this.currentPipeline = resp.data;
        }
      }
    );
  }

  @action.bound setUploadedPipelineData = (uploadedPipelineData: UploadedPipelineDataType | null) => {
    this.uploadedPipelineData = uploadedPipelineData;
  }

  @action.bound setUploadedPipelineFromDcat = (dcatId: string) => {
    axios.get(`${flaskUrl}/pipeline/dcat/${dcatId}`).then(
      (resp) => {
        if (resp.data.error) {
          message.info(`An error has occurred: ${resp.data.error}`); 
        } else {
          this.uploadedPipelineData = resp.data.data;
          this.graphCreated = true;
        }
      }
    );
  }

  @action.bound setGraphCreated = (graphCreated: boolean) => {
    this.graphCreated = graphCreated;
  }

  @action.bound createPipeline = (
    pipelineName: string, pipelineDescription: string,
    graphNodes: NodeType[], graphEdges: EdgeType[]
  ) => {
    axios.post(`${flaskUrl}/pipeline/create`, {
      name: pipelineName,
      description: pipelineDescription,
      nodes: graphNodes,
      edges: graphEdges
    }).then(
      (resp) => {
        if (resp.data.error) {
          message.info(`An error has occurred: ${resp.data.error}`); 
        }
      }
    );
  }
};


