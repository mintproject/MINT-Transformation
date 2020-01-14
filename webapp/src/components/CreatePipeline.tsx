import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { message, Upload, Icon, Row, Button, Tabs, Input, Col, Dropdown, Menu } from "antd";
import MyLayout from "./Layout";
import { UploadedPipelineDataType, NodeType, EdgeType } from "../store/PipelineStore"
import "antd/dist/antd.css";
import { UploadFile, UploadChangeParam } from "antd/lib/upload/interface";
import { RouterProps } from "react-router";
import { flaskUrl, AdapterType } from "../store/AdapterStore";
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
  graphCreated: boolean,
  setUploadedPipelineData: (uploadedPipeline: UploadedPipelineDataType | null) => any,
  setGraphCreated: (graphCreated: boolean) => any,
  setUploadedPipelineFromDcat: (dcatId: string) => any,
  location: Location,
  createPipeline: (pipelineName: string, pipelineDescription: string, graphNodes: NodeType[], graphEdges: EdgeType[]) => any,
  adapters: AdapterType[],
  getAdapters: () => any,
}
interface CreatePipelineState {
  currentFileList: UploadFile[],
  pipelineName: string,
  pipelineDescription: string,
  selected: string | null,
  graphNodes: INode[],
  graphEdges: IEdge[],
  currentInput: string,
  currentAction: string,
  currentAdapter: string,
  currentToNode: string,
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
    currentInput: "",
    currentAction: "",
    currentAdapter: "",
    currentToNode: ""
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

  componentDidUpdate(prevProps: CreatePipelineProps) {
    if (
      prevProps.uploadedPipelineData !== this.props.uploadedPipelineData
      && this.props.uploadedPipelineData !== null
    ) {
      this.setState({
        graphNodes: this.createGraphNodes(this.props.uploadedPipelineData.nodes),
        graphEdges: this.createGraphEdges(this.props.uploadedPipelineData.edges),
      });
    }
  }

  handleFileChange = (info: UploadChangeParam<UploadFile>) => {
    const fileList = [...info.fileList];
    const file = fileList.slice(-1)[0];
    if (file.response && file.response.error) {
      message.info(`${file.response.error}`);
    } else if (file.response && file.response.data) {
      const { data } = file.response;
      this.props.setUploadedPipelineData(data);
      this.props.setGraphCreated(true);
      this.setState({
        graphEdges: this.createGraphEdges(data.edges),
        graphNodes: this.createGraphNodes(data.nodes),
      })
    }
    this.setState({ currentFileList: [file] });
  }

  handleSubmit = () => {
    const { pipelineName, pipelineDescription, graphEdges, graphNodes } = this.state;
    this.props.createPipeline(
      pipelineName, pipelineDescription,
      this.createNodes(graphNodes), this.createEdges(graphEdges)
    );
    this.props.setUploadedPipelineData(null);
    this.props.setGraphCreated(false);
    this.props.history.push('/pipelines');
  }

  handleCancel = () => {
    this.props.setUploadedPipelineData(null);
    this.props.setGraphCreated(false);
    this.setState({
      currentFileList: [],
      pipelineName: "",
      pipelineDescription: "",
      selected: null,
      graphNodes: [],
      graphEdges: [],
      currentInput: "",
      currentAction: "",
      currentAdapter: ""
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

  createNodes = (nodes: INode[]) => {
    return nodes.map(n => ({
      id: parseInt(n.id),
      adapter: n.adapter
    }))
  }

  findBoundingBoxGraphNodes = () => {
    const { graphNodes } = this.state;
    var maxX = Number.NEGATIVE_INFINITY, maxY = Number.NEGATIVE_INFINITY;
    var minX = Number.POSITIVE_INFINITY, minY = Number.POSITIVE_INFINITY;
    var maxId = -1;
    for (var i = 0; i < graphNodes.length; i++) {
      const nodeId = parseInt(graphNodes[i].id);
      const nodeX = Number(graphNodes[i].x);
      const nodeY = Number(graphNodes[i].y);
      if (nodeId > maxId) {
        maxId = nodeId;
      }
      if (nodeX > maxX) {
        maxX = nodeX;
      }
      if (nodeX < minX) {
        minX = nodeX;
      }
      if (nodeY > maxY) {
        maxY = nodeY;
      }
      if (nodeY < minY) {
        minY = nodeY;
      }
    }
    if (graphNodes.length < 1) {
      maxX = 300;
      maxY = 300;
      minX = 300;
      minY = 300;
    }
    return { maxX, maxY, minX, minY, maxId }
  }

  createGraphEdges = (edges: EdgeType[]) => {
    return edges.map((e, idx) => ({
      target: `${e.target}`,
      source: `${e.source}`,
      type: "emptyEdge"
    }))
  }

  createEdges = (edges: IEdge[]) => {
    return edges.map(e => ({
      source: parseInt(e.source),
      target: parseInt(e.target)
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

  createAdapterMenu = () => {
    return (<Menu onClick={({ item }) => {
      this.setState({ currentAdapter: item.props.children });
      }}>
      {this.props.adapters.map((ad, idx) => {
        return (<Menu.Item key={`ad-${idx}`}>
          {ad.id}
        </Menu.Item>);
      })}
    </Menu>);
  };

  createNodeMenu = () => {
    return (<Menu onClick={({ item }) => {
      this.setState({ currentToNode: item.props.children });
      }}>
      {this.state.graphNodes.filter(n => n.id !== this.state.selected).map((node, idx) => {
        return (<Menu.Item key={`node-${idx}`}>
          {node.id}
        </Menu.Item>);
      })}
    </Menu>);
  };

  createNodeEdgeMenu = () => {
    const outgoingEdges = this.state.graphEdges.filter(e => e.source === this.state.selected);
    const targetNodeIds = outgoingEdges.map(e => e.target);
    const targetNodes: INode[] = this.state.graphNodes.filter(n => targetNodeIds.includes(n.id));
    return (<Menu onClick={({ item }) => {
      this.setState({ currentToNode: item.props.children });
      }}>
      {
      targetNodes.map((node, idx) => {
        return (<Menu.Item key={`node-${idx}`}>
          {node.id}
        </Menu.Item>);
      })}
    </Menu>);
  };

  onClear = () => {
    this.setState({
      selected: null,
      currentInput: "",
      currentAction: "",
      currentAdapter: "",
      currentToNode: ""
    });
  }

  render() {
    const { graphCreated } = this.props;
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
            defaultActiveKey="adapters"
            // tabPosition="left"
            style={{ overflowY: "auto", height: "100%" }}
          >
            <TabPane tab="Adapters" key="adapters" style={{ height: "600px" }}>
              <Row style={{ margin: "20px 0px"}}>
                <Col span={16}>
                  { _.isEmpty(this.state.selected) && _.isEmpty(this.state.currentAction) ?
                    <p>
                      <b>* Click on node to edit</b> OR <Button
                        style={{ margin: "5px" }}
                        onClick={() => this.setState({ currentAction: "add-a-new-node"})}
                      >Add A New Node</Button>
                    </p>
                    : null
                  }
                  { this.state.selected && _.isEmpty(this.state.currentAction) ? <span>
                    <p>{`Select an action on Node-${this.state.selected}: `}</p>
                    <Button
                      style={{ margin: "5px" }}
                      onClick={() => this.setState({ currentAction: "add-a-new-node"})}
                    >Add A New Node</Button>
                    <Button
                      style={{ margin: "5px" }}
                      onClick={() => this.setState({ currentAction: "add-a-new-edge"})}
                    >Add A New Edge</Button>
                    <Button
                      style={{ margin: "5px" }}
                      onClick={() => this.setState({ currentAction: "delete-this-node"})}
                    >Delete This Node</Button>
                    <Button
                      style={{ margin: "5px" }}
                      onClick={() => this.setState({ currentAction: "delete-its-edge"})}
                    >Delete Its Edge</Button>
                    <Button
                      style={{ margin: "5px" }}
                      onClick={() => this.setState({ selected: null })}
                      type="dashed"
                    >Cancel</Button>
                  </span> : null}
                  { this.state.currentAction === "add-a-new-node"
                    ? <span>
                      <p>Add A New Node: </p>
                      <Dropdown overlay={this.createAdapterMenu()}>
                        { this.state.currentAdapter
                        ? <b>{`Selected Adapter: ${this.state.currentAdapter}`}</b>
                        : <Button><b>Select An Adapter</b></Button>}
                      </Dropdown>
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        disabled={_.isEmpty(this.state.currentAdapter)}
                        onClick={() => {
                          console.log("adding a new node to the graph");
                          const { maxX, maxY, minX, minY, maxId } = this.findBoundingBoxGraphNodes();
                          console.log(maxX, maxY, minX, minY);
                          const newNode: INode = {
                            id: `${maxId + 1}`,
                            title: `${this.state.currentAdapter}`,
                            // type: "empty",
                            x: 200 + (maxX + minX)/2,
                            y: (maxY + minY)/2,
                            adapter: this.props.adapters.filter(ad => ad.id === this.state.currentAdapter)[0]
                          };
                          const newNodes = this.state.graphNodes;
                          newNodes.push(newNode);
                          this.setState({
                            graphNodes: newNodes,
                            selected: null,
                            currentAction: "",
                            currentAdapter: "",
                            currentInput: ""
                          });
                        }}
                        type="primary"
                      >OK</Button>
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        onClick={this.onClear}
                      >Clear</Button>
                    </span> : null
                  }
                  { this.state.currentAction === "add-a-new-edge"
                    ? <span>
                      <p>{`Add A New Edge From Node-${this.state.selected} To:`} </p>
                      <Dropdown overlay={this.createNodeMenu()}>
                        { this.state.currentToNode
                        ? <b>{`Adding A New Edge From Node-${this.state.selected} To Node-${this.state.currentToNode}`}</b>
                        : <Button><b>Select Node Index</b></Button>}
                      </Dropdown>
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        disabled={_.isEmpty(this.state.currentToNode)}
                        onClick={() => {
                          console.log("adding a new edge to the graph");
                          const newEdges = this.state.graphEdges;
                          newEdges.push({
                            target: this.state.currentToNode,
                            source: this.state.selected || "",
                            type: "empty"
                          });
                          this.setState({
                            graphEdges: newEdges,
                            selected: null,
                            currentAction: "",
                            currentAdapter: "",
                            currentInput: "",
                            currentToNode: ""
                          });
                        }}
                        type="primary"
                      >OK</Button>
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        onClick={this.onClear}
                      >Clear</Button>
                    </span> : null
                  }
                  { this.state.currentAction === "delete-this-node"
                    ? <span>
                      <p>Are you sure you want to delete this node? All inputs/edges of this node will be lost.</p>
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        type="danger"
                        onClick={() => {
                          console.log("deleting this node in the graph");
                          const { selected } = this.state;
                          const newNodes = this.state.graphNodes.filter(n => n.id !== selected);
                          const newEdges = this.state.graphEdges.filter(e => e.target !== selected && e.source !== selected );
                          this.setState({
                            graphEdges: newEdges,
                            graphNodes: newNodes,
                            selected: null,
                            currentInput: "",
                            currentAction: "",
                            currentAdapter: ""
                          })
                        }}
                      >Yes</Button>
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        onClick={this.onClear}
                        type="primary"
                      >No</Button>
                    </span> : null
                  }
                  { this.state.currentAction === "delete-its-edge"
                    ? <span>
                      <p>{`Select An Edge From Node-${this.state.selected}:`} </p>
                      <Dropdown overlay={this.createNodeEdgeMenu()}>
                        { this.state.currentToNode
                        ? <b>{`Deleting The Edge From Node-${this.state.selected} To Node-${this.state.currentToNode}`}</b>
                        : <Button><b>Select Node Index</b></Button>}
                      </Dropdown>
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        type="danger"
                        onClick={() => {
                          console.log("deleting this node in the graph");
                          const { selected, currentToNode } = this.state;
                          const newEdges = this.state.graphEdges.filter(e => e.target !== currentToNode && e.source !== selected );
                          this.setState({
                            graphEdges: newEdges,
                            selected: null,
                            currentInput: "",
                            currentAction: "",
                            currentAdapter: "",
                            currentToNode: ""
                          })
                        }}
                        disabled={this.state.currentToNode === ""}
                      >Yes</Button>
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        onClick={this.onClear}
                        type="primary"
                      >No</Button>
                    </span> : null
                  }
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
                <Col span={16} style={{ height: "50vh" }}>
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
                        <text textAnchor="middle" x={0} y={0} fontSize="smaller">
                          {!!title && <tspan x={0} dy={-lineOffset} opacity="1">{title}</tspan>}
                          {!!id && <tspan x={0} dy={2 * lineOffset} opacity="1">{id}</tspan>}
                        </text>
                      );
                    }}
                    renderNode={({ selected }) => {
                      // console.log(selected);
                      return (
                        <g className="shape" height={100} width={100}>
                          <rect
                            x={-50}
                            y={-50}
                            width={100}
                            height={100}
                            fill="#D9EE87"
                            fillOpacity="1"
                          />
                        </g>
                      );
                    }}
                    maxZoom={1}
                  />
                </Col>
                <Col span={7}>
                  { selectedAdapter !== null && selectedAdapter !== undefined ? <p style={{ margin: "20px 20px"}}>
                    • <b><u>Function Name</u></b>: {selectedAdapter.id}<br/>
                    • <b><u>Description</u></b>: {selectedAdapter.description}<br/>
                  </p> : null}
                  <TextArea
                    style={{ margin: "20px 20px"}}
                    rows={15}
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
                <TextArea
                  rows={16}
                  value={this.state.pipelineDescription}
                  onChange={({ target }) => this.setState({ pipelineDescription: target.value})}
                  placeholder="Enter Pipeline Description"
                />
              </Row>
              {/* <Row style={{ margin: "20px 10px"}}>
                <pre>{JSON.stringify(uploadedPipelineConfig, null, 2)}</pre>
              </Row> */}
            </TabPane>
          </Tabs>
        </MyLayout>
      );
    }
  }
}

export default CreatePipelineComponent;
