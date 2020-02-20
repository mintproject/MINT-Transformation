import { observable, action } from "mobx";
import axios from "axios";
import { AdapterType, flaskUrl, AdapterInputType } from "./AdapterStore";
import { ErrorStore } from "./ErrorStore";
import {
  INode, IEdge,
} from "react-digraph";

// FIXME: settle down on the final format of pipeline object:
// metadata + list of adapters?
export type PipelineType = {
  name: string,
  description: string,
  start_timestamp: string,
  status: string,
  end_timestamp: string,
  config: string,
  id: string,
  adapters?: AdapterType[],
  output?: string
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
  constructor(errorStore: ErrorStore) {
    this.errorStore = errorStore;
  }

  errorStore: ErrorStore
  @observable pipelines: PipelineType[] = [];
  @observable currentPipeline: PipelineType | null = null;
  @observable uploadedPipelineData: UploadedPipelineDataType | null = null;
  @observable graphCreated: boolean = false;
  @observable selectedNode: INode | null = null;
  @observable selectedEdge: IEdge | null = null;
  @observable graphNodes: INode[] = [];
  @observable graphEdges: IEdge[] = [];

  @action.bound startFetch = () => {
    this.errorStore.isLoading = true;
    console.log("inside start fetch!");
  }

  @action.bound successFetch = () => {
    this.errorStore.isLoading = false;
    this.errorStore.hasFailed = false;
    console.log("inside success fetch!");
  }

  @action.bound failedFetch = (errorData: Error) => {
    console.log("inside failed fetch!");
    this.errorStore.isLoading = false;
    this.errorStore.hasFailed = true;
    this.errorStore.errorData = errorData;
  }

  @action.bound getPipelines = () => {
    Promise.resolve().then(this.startFetch).then(() =>
      axios.get(`${flaskUrl}/pipelines`)
      .then(
        resp => {
          this.pipelines = resp.data;
          this.successFetch();
        },
        error => {
          this.failedFetch(error.response);
          /* 
          if (error.response) {
              // The request was made and the server responded with a
              // status code that falls out of the range of 2xx
              console.log(error.response.data);
              console.log(error.response.status);
              console.log(error.response.headers);
          } else if (error.request) {
              //  * The request was made but no response was received, `error.request`
              //  * is an instance of XMLHttpRequest in the browser and an instance
              //  * of http.ClientRequest in Node.js
              console.log(error.request);
          } else {
              // Something happened in setting up the request and triggered an Error
              console.log('Error', error.message);
          }
          console.log(error.config);
          */
      })
    )
  }

  @action.bound getPipeline = (pipelineId: string) => {
    Promise.resolve().then(this.startFetch).then(() =>
      axios.get(`${flaskUrl}/pipelines/${pipelineId}`)
      .then(
        resp => {
          this.currentPipeline = resp.data;
          this.successFetch();
        },
        error => {
          this.failedFetch(error);
    }));
  }

  @action.bound setUploadedPipelineData = (uploadedPipelineData: UploadedPipelineDataType | null) => {
    this.uploadedPipelineData = uploadedPipelineData;
  }

  @action.bound setUploadedPipelineFromDcat = (dcatId: string) => {
    Promise.resolve().then(this.startFetch).then(() =>
      axios.get(`${flaskUrl}/pipeline/dcat/${dcatId}`)
      .then(
        resp => {
          this.uploadedPipelineData = resp.data.data;
          this.graphCreated = true;
          this.successFetch();
        },
        error => {
          this.failedFetch(error);
    }));
  }

  @action.bound setGraphCreated = (graphCreated: boolean) => {
    this.graphCreated = graphCreated;
  }

  @action.bound createPipeline = (
    pipelineName: string, pipelineDescription: string,
    graphNodes: NodeType[], graphEdges: EdgeType[]
  ) => {
    Promise.resolve().then(this.startFetch).then(() =>
      axios.post(`${flaskUrl}/pipeline/create`, {
        name: pipelineName,
        description: pipelineDescription,
        nodes: graphNodes,
        edges: graphEdges
      }).then(
        resp => {
          this.successFetch();
        },
        error => {
          this.failedFetch(error);
    }));
  }

  @action.bound setGraphNodes = (nodes: INode[]) => {
    this.graphNodes = nodes;
  }

  @action.bound setGraphEdges = (edges: IEdge[]) => {
    this.graphEdges = edges;
  }

  @action.bound setSelectedNode = (node: INode | null) => {
    this.selectedNode = node;
    this.selectedEdge = null;
  }

  @action.bound setSelectedEdge = (edge: IEdge | null) => {
    this.selectedEdge = edge;
    this.selectedNode = null;
  }
};
