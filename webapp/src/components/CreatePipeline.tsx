import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { message, Upload, Icon, Row, Button, Tabs, Input, Col } from "antd";
import MyLayout from "./Layout";
import { UploadedPipelineDataType, NodeType, EdgeType } from "../store/PipelineStore"
import "antd/dist/antd.css";
import { UploadFile, UploadChangeParam } from "antd/lib/upload/interface";
import { RouterProps } from "react-router";
import { flaskUrl } from "../store/AdapterStore";
import queryString from 'query-string';
import {
  GraphView, INode, IEdge,
} from "react-digraph";
import _ from "lodash";
const { TextArea } = Input;
const { TabPane } = Tabs;

const GraphConfig =  {
  NodeTypes: {
    empty: { // required to show empty nodes
      typeText: "wat",
      shapeId: "#empty", // relates to the type property of a node
      shape: (
        <symbol viewBox="0 0 50 50" id="empty" key="0">
          {/* <circle cx="50" cy="50" r="50"></circle> */}
          <rect x="0" y="0" height="50" width="50"/>
        </symbol>
      )
    },
  },
  NodeSubtypes: {},
  EdgeTypes: {
    emptyEdge: {  // required to show empty edges
      shapeId: "#emptyEdge",
      shape: (
        <symbol viewBox="0 0 50 50" id="emptyEdge" key="0">
          <circle cx="25" cy="25" r="8" fill="currentColor"> </circle>
        </symbol>
      )
    }
  }
}

interface CreatePipelineProps extends RouterProps {
  uploadedPipelineData: UploadedPipelineDataType | null,
  uploadedPipelineConfig: object | null,
  setUploadedPipelineData: (uploadedPipeline: UploadedPipelineDataType | null) => any,
  setUploadedPipelineConfig: (uploadedPipelineConfig: object | null) => any,
  setUploadedPipelineFromDcat: (dcatId: string) => any,
  location: Location,
  createPipeline: (pipelineName: string, pipelineDescription: string, pipelineConfig: object) => any,
}
interface CreatePipelineState {
  currentFileList: UploadFile[],
  pipelineName: string,
  pipelineDescription: string,
  selected: string | null,
  graphNodes: INode[],
  graphEdges: IEdge[],
  currentInput: string,
}

@inject((stores: IStore) => ({
  uploadedPipelineData: stores.pipelineStore.uploadedPipelineData,
  uploadedPipelineConfig: stores.pipelineStore.uploadedPipelineConfig,
  location: stores.routing.location,
  createPipeline: stores.pipelineStore.createPipeline,
  setUploadedPipelineConfig: stores.pipelineStore.setUploadedPipelineConfig,
  setUploadedPipelineData: stores.pipelineStore.setUploadedPipelineData,
  setUploadedPipelineFromDcat: stores.pipelineStore.setUploadedPipelineFromDcat,
}))
@observer
export class CreatePipelineComponent extends React.Component<
  CreatePipelineProps,
  CreatePipelineState
> {
  public state: CreatePipelineState = {
    currentFileList: [],
    pipelineName: "",
    pipelineDescription: "",
    selected: null,
    graphNodes: [],
    graphEdges: [],
    currentInput: ""
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
  }

  handleFileChange = (info: UploadChangeParam<UploadFile>) => {
    const fileList = [...info.fileList];
    const file = fileList.slice(-1)[0];
    if (file.response && file.response.error) {
      message.info(`${file.response.error}`);
    } else if (file.response && file.response.data) {
      const { data, config } = file.response;
      this.props.setUploadedPipelineData(data);
      this.props.setUploadedPipelineConfig(config);
      this.setState({
        graphEdges: this.createGraphEdges(data.edges),
        graphNodes: this.createGraphNodes(data.nodes),
      })
    }
    this.setState({ currentFileList: [file] });
  }

  handleSubmit = () => {
    const { pipelineName, pipelineDescription } = this.state;
    const { uploadedPipelineConfig } = this.props;
    if (uploadedPipelineConfig === null) {
      return;
    }
    this.props.createPipeline(pipelineName, pipelineDescription, uploadedPipelineConfig);
    this.props.setUploadedPipelineData(null);
    this.props.setUploadedPipelineConfig(null);
    this.props.history.push('/pipelines');
  }

  handleCancel = () => {
    this.props.setUploadedPipelineData(null);
    this.props.setUploadedPipelineConfig(null);
    this.setState({
      currentFileList: [],
      pipelineName: "",
      pipelineDescription: "",
      selected: null,
      graphNodes: [],
      graphEdges: [],
      currentInput: ""
    });
    this.props.history.push('/pipeline/create');
  }

  createGraphNodes = (nodes: NodeType[]) => {
    return nodes.map((n, idx) => ({
      id: `${n.id}`,
      title: n.adapter.id,
      // type: "empty",
      x: 100 + 200*idx,
      y: 100,
      adapter: n.adapter
    }))
  }

  createGraphEdges = (edges: EdgeType[]) => {
    return edges.map((e, idx) => ({
      target: `${e.target}`,
      source: `${e.source}`,
      type: "emptyEdge"
    }))
  }

  onUpdateNode = (node: INode) => {
    const selectedAdapter =  this.state.graphNodes.filter(n => n.id === node.id)[0].adapter;
    this.setState({
      selected: node.id,
      graphNodes: this.state.graphNodes.map(v => v.id === node.id ? node : v),
      currentInput: JSON.stringify(_.get(selectedAdapter, "inputs"), null, 2)
    });
  };

  render() {
    const { uploadedPipelineData, uploadedPipelineConfig } = this.props;
    const selectedNode = this.state.graphNodes.filter(n => n.id === this.state.selected);
    const selectedAdapter = _.get(
      selectedNode[0], "adapter"
    );
    var saveDisabled = false;
    try {
      JSON.parse(this.state.currentInput);
    } catch (e) {
      saveDisabled = true;
    }

    if (uploadedPipelineData === null) {
      return <MyLayout>
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
      </MyLayout>
    } else {
      return (
        <MyLayout> 
          <Tabs
            defaultActiveKey="adapters"
            // tabPosition="left"
            style={{ overflowY: "auto", height: "100%" }}
          >
            <TabPane tab="Adapters" key="adapters" style={{ height: "600px" }}>
              <Row style={{ margin: "20px 0px"}}>
                <Col span={16}>
                  <p>* Click on adapter component to edit.</p>
                </Col>
                <Col span={8}>
                  <Button
                    key="discard" onClick={this.handleCancel}
                    style={{ margin: "10px", float: "right" }}
                  >
                    Discard
                  </Button>
                  <Button
                    key="submit" type="primary"
                    onClick={this.handleSubmit}
                    style={{ margin: "10px", float: "right" }}
                  >
                    Submit
                  </Button>
                </Col>
              </Row>
              <Row>
                <Col span={16} style={{ height: "60vh" }}>
                  <GraphView
                    ref='GraphView'
                    nodeKey="id"
                    nodes={this.state.graphNodes}
                    edges={this.state.graphEdges}
                    selected={{}}
                    nodeTypes={GraphConfig.NodeTypes}
                    nodeSubtypes={GraphConfig.NodeSubtypes}
                    edgeTypes={GraphConfig.EdgeTypes}
                    // onSelectNode={this.onSelectNode}
                    // onCreateNode={this.onCreateNode}
                    onUpdateNode={this.onUpdateNode}
                    // onDeleteNode={this.onDeleteNode}
                    // onSelectEdge={this.onSelectEdge}
                    // onCreateEdge={this.onCreateEdge}
                    // onSwapEdge={this.onSwapEdge}
                    // onDeleteEdge={this.onDelet() => console.log("does nothing")
                    onSelectNode={() => console.log("does nothing")}
                    onCreateNode={() => console.log("does nothing")}
                    // onUpdateNode={() => console.log("does nothing")}
                    onDeleteNode={() => console.log("does nothing")}
                    onSelectEdge={() => console.log("does nothing")}
                    onCreateEdge={() => console.log("does nothing")}
                    onSwapEdge={() => console.log("does nothing")}
                    onDeleteEdge={() => console.log("does nothing")}
                    renderNodeText={(data: any, id: string | number, isSelected: boolean) => {
                      const { title } = data;
                      const lineOffset = 18;
                      // console.log(data)
                      return (
                        <text textAnchor="middle" x={0} y={0}>
                          {!!title && <tspan x={0} dy={-lineOffset} opacity="1">{title}</tspan>}
                        </text>
                      );
                    }}
                    renderNode={({ data }) => {
                      // console.log("hello")
                      return (
                        <g className="shape" height={100} width={100}>
                          <rect
                            x={-50}
                            y={-50}
                            width={100}
                            height={100}
                            fill="#044B94"
                            fillOpacity="1"
                          />
                        </g>
                      );
                    }}
                  />
                </Col>
                <Col span={7}>
                  <TextArea
                    style={{ margin: "20px 20px"}}
                    rows={20}
                    defaultValue="Details of adapter..."
                    value={this.state.currentInput}
                    onChange={({ target }) => {
                      this.setState({ currentInput: target.value })
                    }}
                  />
                  <Button
                    disabled={selectedAdapter === null || saveDisabled}
                    onClick={() => {
                      const newAdapter = Object.assign(selectedNode[0].adapter, { inputs: JSON.parse(this.state.currentInput)});
                      const newNode = Object.assign(selectedNode[0], { adapter: newAdapter });
                      this.setState({
                        graphNodes: this.state.graphNodes.map(v => v.id === this.state.selected ? newNode : v)
                      });
                    }}
                    style={{ float: "right" }}
                  > Save </Button>
                </Col>
              </Row>
              {/* <pre>{JSON.stringify(uploadedPipelineData, null, 2)}</pre> */}
            </TabPane>
            <TabPane tab="Metadata" key="metadata">
              <Row style={{ margin: "20px 0px"}}>
                <Col span={16}>
                  <Input
                    value={this.state.pipelineName}
                    onChange={(event: React.ChangeEvent<HTMLInputElement>) => this.setState({ pipelineName: event.target.value})}
                    placeholder="Enter Pipeline Name"
                    style={{ margin: "10px" }}
                  />
                </Col>
                <Col span={8}>
                  <Button
                    key="discard" onClick={this.handleCancel}
                    style={{ margin: "10px", float: "right" }}
                  >
                    Discard
                  </Button>
                  <Button
                    key="submit" type="primary"
                    onClick={this.handleSubmit}
                    style={{ margin: "10px", float: "right" }}
                  >
                    Submit
                  </Button>
                </Col>
              </Row>
              <Row style={{ margin: "20px 10px"}}>
                <Input
                  value={this.state.pipelineDescription}
                  onChange={(event: React.ChangeEvent<HTMLInputElement>) => this.setState({ pipelineDescription: event.target.value})}
                  placeholder="Enter Pipeline Description"
                />
              </Row>
              <Row style={{ margin: "20px 10px"}}>
                <pre>{JSON.stringify(uploadedPipelineConfig, null, 2)}</pre>
              </Row>
            </TabPane>
          </Tabs>
        </MyLayout>
      );
    }
  }
}

export default CreatePipelineComponent;
