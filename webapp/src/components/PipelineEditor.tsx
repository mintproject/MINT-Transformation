import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { message, Upload, Icon, Row, Button, Tabs, Input, Col, Modal } from "antd";
import MyLayout from "./Layout";
import { UploadedPipelineDataType, NodeType, EdgeType } from "../store/PipelineStore"
import "antd/dist/antd.css";
import { UploadFile, UploadChangeParam } from "antd/lib/upload/interface";
import { RouterProps } from "react-router";
import { flaskUrl, AdapterType } from "../store/AdapterStore";
import queryString from 'query-string';
import {
  INode, IEdge,
} from "react-digraph";
import PipelineGraph from "./PipelineGraph";
import AdapterInputs from "./AdapterInputs";
import EdgeDetail from "./EdgeDetail";

const { TextArea } = Input;
const { TabPane } = Tabs;

interface PipelineEditorProps extends RouterProps {
  uploadedPipelineData: UploadedPipelineDataType | null,
  graphCreated: boolean,
  setUploadedPipelineData: (uploadedPipeline: UploadedPipelineDataType | null) => any,
  setGraphCreated: (graphCreated: boolean) => any,
  setUploadedPipelineFromDcat: (dcatId: string) => any,
  location: Location,
  createPipeline: (pipelineName: string, pipelineDescription: string, graphNodes: NodeType[], graphEdges: EdgeType[]) => any,
  adapters: AdapterType[],
  getAdapters: () => any,
  selectedNode: INode | null,
  selectedEdge: IEdge | null,
  graphNodes: INode[],
  graphEdges: IEdge[],
  setGraphNodes: (nodes: INode[]) => any,
  setGraphEdges: (edges: IEdge[]) => any,
  setSelectedNode: (node: INode | null) => any,
  setSelectedEdge: (edge: IEdge | null) => any,
}
interface PipelineEditorState {
  currentFileList: UploadFile[],
  pipelineName: string,
  pipelineDescription: string,
  willSubmit: boolean,
}

@inject((stores: IStore) => ({
  uploadedPipelineData: stores.pipelineStore.uploadedPipelineData,
  graphCreated: stores.pipelineStore.graphCreated,
  location: stores.routing.location,
  createPipeline: stores.pipelineStore.createPipeline,
  setGraphCreated: stores.pipelineStore.setGraphCreated,
  setUploadedPipelineData: stores.pipelineStore.setUploadedPipelineData,
  setUploadedPipelineFromDcat: stores.pipelineStore.setUploadedPipelineFromDcat,
  adapters: stores.adapterStore.adapters,
  getAdapters: stores.adapterStore.getAdapters,
  selectedNode: stores.pipelineStore.selectedNode,
  selectedEdge: stores.pipelineStore.selectedEdge,
  graphNodes: stores.pipelineStore.graphNodes,
  graphEdges: stores.pipelineStore.graphEdges,
  setGraphNodes: stores.pipelineStore.setGraphNodes,
  setGraphEdges: stores.pipelineStore.setGraphEdges,
  setSelectedNode: stores.pipelineStore.setSelectedNode,
  setSelectedEdge: stores.pipelineStore.setSelectedEdge,
}))
@observer
export class PipelineEditorComponent extends React.Component<
  PipelineEditorProps,
  PipelineEditorState
> {
  public state: PipelineEditorState = {
    currentFileList: [],
    pipelineName: "",
    pipelineDescription: "",
    willSubmit: false,
  };

  componentDidMount() {
    if (this.props.location.search) {
      const params = queryString.parse(this.props.location.search)
      if (params && params.dcatId && typeof params.dcatId === "string") {
        this.props.setUploadedPipelineFromDcat(params.dcatId)
      }
    } else {
      // redirect to create page to upload
      this.props.history.push('/pipeline/create');
    }
    this.props.getAdapters();
  }

  componentDidUpdate(prevProps: PipelineEditorProps) {
    if (
      // Integration with dcat
      prevProps.uploadedPipelineData !== this.props.uploadedPipelineData
      && this.props.uploadedPipelineData !== null
    ) {
      const { uploadedPipelineData, setGraphNodes, setGraphEdges } = this.props;
      setGraphEdges(this.createGraphEdges(uploadedPipelineData.edges));
      setGraphNodes(this.createGraphNodes(uploadedPipelineData.nodes));
    }
  }

  handleFileChange = (info: UploadChangeParam<UploadFile>) => {
    const fileList = [...info.fileList];
    const file = fileList.slice(-1)[0];
    if (file.response && file.response.error) {
      message.info(`${file.response.error}`);
    } else if (file.response && file.response.data) {
      const { data } = file.response;
      // this.props.setUploadedPipelineData(data);
      this.props.setGraphCreated(true);
      this.props.setGraphNodes(this.createGraphNodes(data.nodes));
      this.props.setGraphEdges(this.createGraphEdges(data.edges));
    }
    this.setState({ currentFileList: [file] });
  }

  handleSubmit = () => {
    const { pipelineName, pipelineDescription } = this.state;
    const { graphEdges, graphNodes } = this.props;
    this.props.createPipeline(
      pipelineName, pipelineDescription,
      this.createNodes(graphNodes), this.createEdges(graphEdges)
    );
    this.props.setUploadedPipelineData(null);
    this.props.setGraphCreated(false);
    setTimeout(() => {this.props.history.push('/pipelines');}, 1000);
  }

  handleCancel = () => {
    this.props.setUploadedPipelineData(null);
    this.props.setGraphNodes([]);
    this.props.setGraphEdges([]);
    this.props.setGraphCreated(false);
    this.props.setSelectedNode(null);
    this.setState({
      currentFileList: [],
      pipelineName: "",
      pipelineDescription: "",
    });
    this.props.history.push('/pipeline/create');
  }

  createGraphNodes = (nodes: NodeType[]) => {
    return nodes.map((n, idx) => {
      const title = n.adapter.split(".")[1];
      const currAdapter = this.props.adapters.filter(ad => ad.id === title)[0];
      return ({
        id: n.id,
        title: title,
        // type: "empty",
        x: 100 + 200*idx,
        y: 100,
        adapter: {
          id: title,
          description: currAdapter.description,
          inputs: n.inputs,
          outputs: n.outputs
        }
      });
    })
  }

  createNodes = (nodes: INode[]) => {
    return nodes.map(n => ({
      id: n.id,
      adapter: `funcs.${n.title}`,
      inputs: n.adapter.inputs,
      outputs: n.adapter.outputs,
      // FIXME: missing comment
    }))
  }

  createGraphEdges = (edges: EdgeType[]) => {
    return edges.map(e => ({
      ...e,
      type: "emptyEdge",
    }))
  }

  createEdges = (edges: IEdge[]) => {
    return edges.map(e => ({
      source: e.source,
      target: e.target,
      input: e.input,
      output: e.output,
    }))
  }

  render() {
    const { graphCreated, selectedNode } = this.props;
    // const selectedAdapter = selectedNode ? selectedNode.adapter : null;
    if (!graphCreated) {
      return <MyLayout>
        <Col span={12} style={{ textAlign: "center", height: "100%" }}>
          <Button size="large" type="primary" icon="plus"
            onClick={() => this.props.setGraphCreated(true)}
            style={{ marginTop: "30vh" }}
          >Click To Start</Button>
        </Col>
        <Col span={12} style={{ height: "100%" }}>
          <Upload.Dragger
            name="files"
            action={`${flaskUrl}/pipeline/upload_config`}
            accept=".json,.yml"
            onChange={this.handleFileChange}
            multiple={false}
            fileList={this.state.currentFileList}
            style={{ height: "100%" }}
          >
            <p className="ant-upload-drag-icon">
              <Icon type="inbox" />
            </p>
            <p className="ant-upload-text">Click or drag file to this area to upload</p>
            <p className="ant-upload-hint">Support for single upload.</p>
          </Upload.Dragger>
        </Col>
      </MyLayout>
    } else {
      return (
        <MyLayout> 
          <Tabs
            defaultActiveKey="pipeline"
            // tabPosition="left"
            style={{ overflowY: "auto", height: "100%" }}
          >
            <TabPane tab="Pipeline" key="pipeline" style={{ height: "600px" }}>
              <Col span={16} style={{ height: "90%" }}>
                <PipelineGraph
                  selectedNode={selectedNode}
                  graphNodes={this.props.graphNodes}
                  graphEdges={this.props.graphEdges}
                  setGraphNodes={this.props.setGraphNodes}
                  setGraphEdges={this.props.setGraphEdges}
                  setSelectedNode={this.props.setSelectedNode}
                  setSelectedEdge={this.props.setSelectedEdge}
                  adapters={this.props.adapters}
                  graphCreated={this.props.graphCreated}
                />
              </Col>
              <Col span={8}>
                <Row style={{ margin: "20px 0px"}}>
                  <Button
                    key="discard" onClick={this.handleCancel}
                    style={{ margin: "10px", float: "right" }}
                  >
                    Discard
                  </Button>
                  <Button
                    key="submit" type="primary"
                    onClick={() => this.setState({ willSubmit: true })}
                    style={{ margin: "10px", float: "right" }}
                  >
                    Submit
                  </Button>
                  <Modal
                    title="Submitting current pipeline..."
                    visible={this.state.willSubmit}
                    onOk={() => {
                      this.handleSubmit();
                      this.setState({ willSubmit: false })
                    }}
                    onCancel={() => this.setState({ willSubmit: false })}
                  >
                    <Row style={{ margin: "20px 10px"}}>
                      <Input
                        value={this.state.pipelineName}
                        onChange={(event: React.ChangeEvent<HTMLInputElement>) => this.setState({ pipelineName: event.target.value})}
                        placeholder="Enter Pipeline Name: (Optional)"
                      />
                    </Row>
                    <Row style={{ margin: "20px 10px"}}>
                      <TextArea
                        rows={16}
                        value={this.state.pipelineDescription}
                        onChange={({ target }) => this.setState({ pipelineDescription: target.value})}
                        placeholder="Enter Pipeline Description: (Optional)"
                      />
                    </Row>
                  </Modal>
                </Row>
                <Row style={{ margin: "20px 0px"}}>
                  {this.props.selectedNode === null ? null : <AdapterInputs
                    selectedNode={selectedNode}
                    graphNodes={this.props.graphNodes}
                    graphEdges={this.props.graphEdges}
                    setGraphNodes={this.props.setGraphNodes}
                    setGraphEdges={this.props.setGraphEdges}
                    setSelectedNode={this.props.setSelectedNode}
                    adapters={this.props.adapters}
                    graphCreated={this.props.graphCreated}
                  />}
                  {this.props.selectedEdge === null ? null : <EdgeDetail
                    selectedEdge={this.props.selectedEdge}
                    graphNodes={this.props.graphNodes}
                    adapters={this.props.adapters}
                  />}
                </Row>
              </Col>
            </TabPane>
          </Tabs>
        </MyLayout>
      );
    }
  }
}

export default PipelineEditorComponent;
