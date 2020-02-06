import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { message, Upload, Icon, Row, Button, Tabs, Input, Col, Dropdown, Menu, Modal } from "antd";
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
import SubMenu from "antd/lib/menu/SubMenu";
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
  currentAction: string,
  currentAdapter: string,
  currentAdapterName: string,
  currentToNode: string,
  fromNodeOutput: string,
  toNodeInput: string,
  showAdapterSpecs: boolean,
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
    currentAction: "",
    currentAdapter: "",
    currentToNode: "",
    fromNodeOutput: "",
    toNodeInput: "",
    currentAdapterName: "",
    showAdapterSpecs: false
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
      // Integration with dcat
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
      // this.props.setUploadedPipelineData(data);
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
      currentAction: "",
      currentAdapter: "",
      currentToNode: "",
      fromNodeOutput: "",
      toNodeInput: "",
      currentAdapterName: ""
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

  findBoundingBoxGraphNodes = () => {
    const { graphNodes } = this.state;
    var maxX = Number.NEGATIVE_INFINITY, maxY = Number.NEGATIVE_INFINITY;
    var minX = Number.POSITIVE_INFINITY, minY = Number.POSITIVE_INFINITY;
    var maxId = -1;
    for (var i = 0; i < graphNodes.length; i++) {
      const nodeX = Number(graphNodes[i].x);
      const nodeY = Number(graphNodes[i].y);
      if (i > maxId) {
        maxId = i;
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
    if (maxId < 0) {
      maxX = 300;
      maxY = 300;
      minX = 300;
      minY = 300;
    }
    return { maxX, maxY, minX, minY, maxId }
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

  onUpdateNode = (node: INode) => {
    this.setState({
      selected: node.id,
      graphNodes: this.state.graphNodes.map(v => v.id === node.id ? node : v)
    });
  };

  createAdapterMenu = () => {
    const currAdapters = this.props.adapters;
    const currAdapterTypes = currAdapters.map(ad => ad.func_type).filter(
      (v, i, a) => a.indexOf(v) === i
    );
    return (<Menu onClick={({ item }) => {
      this.setState({ currentAdapter: item.props.children });
    }}>
      {currAdapterTypes.map((adt, i) => {
        return (<SubMenu
            key={`adt-${i}`} title={adt}
          >
          {currAdapters.filter(ad => ad.func_type === adt).map((ad, idx) => {
            return (<Menu.Item key={`ad-${idx}`}>
              {_.isEmpty(ad.friendly_name) ? ad.id : ad.friendly_name}
            </Menu.Item>);
          })}
        </SubMenu>)
      })}
    </Menu>);
  };

  createFromNodeOutputMenu = () => {
    const selectedAdapter = this.state.graphNodes.filter(n => n.id === this.state.selected)[0].adapter;
    return (<Menu onClick={({ item }) => {
      this.setState({ fromNodeOutput: item.props.children });
      }}>
      {Object.keys(selectedAdapter.outputs).map((k, idx) => {
        return (<Menu.Item key={`output-${idx}`}>
          {k}
        </Menu.Item>);
      })}
    </Menu>);
  };

  createToNodeMenu = () => {
    return (<Menu onClick={({ item }) => {
      this.setState({ currentToNode: item.props.children });
      }}>
      {this.state.graphNodes.filter(n => n.id !== this.state.selected).map((node, idx) => {
        return (<Menu.Item key={`${idx}`}>
          {node.id}
        </Menu.Item>);
      })}
    </Menu>);
  };

  createToNodeInputMenu = () => {
    const selectedAdapter = this.state.graphNodes.filter(n => n.id === this.state.currentToNode)[0].adapter;
    return (<Menu onClick={({ item }) => {
      this.setState({ toNodeInput: item.props.children });
      }}>
      {Object.keys(selectedAdapter.inputs).map((k, idx) => {
        return (<Menu.Item key={`input-${idx}`}>
          {k}
        </Menu.Item>);
      })}
    </Menu>);
  };

  createNodeEdgeMenu = () => {
    const nodeEdges = this.state.graphEdges.filter(
      e => e.source === this.state.selected || e.target === this.state.selected
    );
    return (<Menu onClick={({ key }) => {
        const updates = key.split("-");
        this.setState({
          selected: updates[0],
          fromNodeOutput: updates[1],
          currentToNode: updates[2],
          toNodeInput: updates[3]
        });
      }}>
      {
      nodeEdges.map((e, idx) => {
        return (<Menu.Item key={`${e.source}-${e.output}-${e.target}-${e.input}`}>
          {`${e.source}.${e.output} => ${e.target}.${e.input}`}
        </Menu.Item>);
      })}
    </Menu>);
  };

  createNodeInput = (selectedAdapter: AdapterType, ip: string, idx: number, optional: boolean) => {
    if (selectedAdapter.inputs[ip].id === "graph") {
      const wiredEdges = this.state.graphEdges.filter(e => e.target === this.state.selected && e.input === "graph");
      return <p key={`input-${idx}`} style={{ margin: "20px 20px"}}>
        {`${ip}: `}
        {!optional ? "* " : null}
        <input
          disabled={true}
          type="text"
          value={
            _.isEmpty(wiredEdges) ? "Only Changeable By Editing Edges"
            : `${wiredEdges[0].source}.${wiredEdges[0].output}`
          }
          style={{
            padding:"5px",
            border:"2px solid",
            borderRadius: "5px",
            width: "100%"
          }}
        />
      </p>
    }
    return (
      <p key={`input-${idx}`} style={{ margin: "20px 20px"}}>
        {`${ip}: `}
        {!optional ? "* " : null}
        <input
          name={ip}
          type="text"
          required={selectedAdapter.inputs[ip].optional}
          value={selectedAdapter.inputs[ip].val || ""}
          onChange={(event: React.ChangeEvent<HTMLInputElement>) => {
            const { currentTarget } = event;
            var newNodes = this.state.graphNodes;
            var newNode = newNodes.filter(n => n.id === this.state.selected)[0];
            newNode.adapter.inputs[currentTarget.name].val = currentTarget.value;
            this.setState({ graphNodes: newNodes });
          }}
          style={{
            padding:"5px",
            border:"2px solid",
            borderRadius: "5px",
            width: "100%"
          }}
        />
      </p>
    );
  }

  onClear = () => {
    this.setState({
      selected: null,
      currentAction: "",
      currentAdapter: "",
      currentToNode: "",
      toNodeInput: "",
      fromNodeOutput: "",
      currentAdapterName: ""
    });
  }

  isAlphaNumericUnderscore = (check: string) => {
    const subStrings = check.split("_");
    for (var i = 0; i < subStrings.length; i++){
      if (subStrings[i].match(/^[0-9a-zA-Z]+$/g) === null) { return false; }
    }
    return true;
  }

  render() {
    const { graphCreated } = this.props;
    const selectedNode = this.state.graphNodes.filter(n => n.id === this.state.selected);
    const selectedAdapter = _.get(
      selectedNode[0], "adapter"
    );

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
                    <p>{`Select an action on ${this.state.selected}: `}</p>
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
                      { this.state.currentAdapter ? <p>
                        Enter adapter name (Please only use "_" as delimiter):
                        <input
                          style={{ margin: "10px" }}
                          value={this.state.currentAdapterName}
                          onChange={({ target }) => this.setState({ currentAdapterName: target.value })}
                        /> 
                      </p> : null}
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        disabled={_.isEmpty(this.state.currentAdapter) || !this.isAlphaNumericUnderscore(this.state.currentAdapterName)}
                        onClick={() => {
                          console.log("adding a new node to the graph");
                          const { maxX, maxY, minX, minY, maxId } = this.findBoundingBoxGraphNodes();
                          const customName = this.state.currentAdapterName ? this.state.currentAdapterName : `Node-${maxId + 1}`;
                          const newNode: INode = {
                            id: customName,
                            title: `${this.state.currentAdapter}`,
                            // type: "empty",
                            x: 300 + (maxX + minX)/2,
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
                            currentAdapterName: ""
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
                      <p>
                        {`Add A New Edge From ${this.state.selected} `}
                        <Dropdown overlay={this.createFromNodeOutputMenu()}>
                          { this.state.fromNodeOutput
                          ? <b>{ `${this.state.fromNodeOutput}`}</b>
                          : <Button><b>Select Node Output</b></Button>}
                        </Dropdown>
                        {` To `}
                        <Dropdown overlay={this.createToNodeMenu()}>
                          { this.state.currentToNode
                          ? <b>{` ${this.state.currentToNode}` }</b>
                          : <Button><b>Select Node Output</b></Button>}
                        </Dropdown>
                        {`   `}
                        { _.isEmpty(this.state.currentToNode) ? null : <Dropdown overlay={this.createToNodeInputMenu()}>
                          { this.state.toNodeInput
                          ? <b>{ `${this.state.toNodeInput}`}</b>
                          : <Button><b>Select Node Input</b></Button>}
                        </Dropdown>}
                        <Button
                          style={{ float: "right", margin: "5px" }}
                          disabled={_.isEmpty(this.state.toNodeInput)}
                          onClick={() => {
                            console.log("adding a new edge to the graph");
                            // FIXME: should we update node fields as well?
                            const newEdges = this.state.graphEdges;
                            newEdges.push({
                              target: this.state.currentToNode,
                              source: this.state.selected || "",
                              type: "empty",
                              input: this.state.toNodeInput,
                              output: this.state.fromNodeOutput
                            });
                            this.setState({
                              graphEdges: newEdges,
                              selected: null,
                              currentAction: "",
                              currentAdapter: "",
                              currentToNode: "",
                              toNodeInput: "",
                              fromNodeOutput: ""
                            });
                          }}
                          type="primary"
                        >OK</Button>
                        <Button
                          style={{ float: "right", margin: "5px" }}
                          onClick={this.onClear}
                        >Clear</Button>
                      </p>
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
                      <p>{`Select An Edge From ${this.state.selected}:`} </p>
                      <Dropdown overlay={this.createNodeEdgeMenu()}>
                        { this.state.currentToNode
                        ? <b>{
                          `Deleting The Edge From ${this.state.selected}.${this.state.fromNodeOutput}
                           To ${this.state.currentToNode}.${this.state.toNodeInput}`}</b>
                        : <Button><b>Select Node Edges</b></Button>}
                      </Dropdown>
                      <Button
                        style={{ float: "right", margin: "5px" }}
                        type="danger"
                        onClick={() => {
                          console.log("deleting this node in the graph");
                          const { selected, currentToNode, fromNodeOutput, toNodeInput } = this.state;
                          // FIXME: should we update graphNodes as well?
                          const newEdges = this.state.graphEdges.filter(
                            e => e.target !== currentToNode || e.source !== selected
                            || e.input !== toNodeInput || e.output !== fromNodeOutput
                          );
                          this.setState({
                            graphEdges: newEdges,
                            selected: null,
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
                <Col span={!_.isEmpty(selectedAdapter) ? 16 : 24} style={{ height: "50vh" }}>
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
                      if (typeof id !== "string") {
                        return;
                      }
                      const customName = id.split("-")[1];
                      return (
                        <text textAnchor="middle" x={0} y={0} fontSize="smaller">
                          {!!title && <tspan x={0} dy={-lineOffset} opacity="1">{title}</tspan>}
                          {!!customName && <tspan x={0} dy={2 * lineOffset} opacity="1">{customName}</tspan>}
                        </text>
                      );
                    }}
                    renderNode={({ selected }) => {
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
                <Col span={!_.isEmpty(selectedAdapter) ? 8 : 1}>
                  { !_.isEmpty(selectedAdapter) ?
                  <React.Fragment>
                    <p style={{ margin: "20px 20px"}}>
                      • <b><u>Function Name</u></b>: {selectedAdapter.id}<br/>
                      • <b><u>Description</u></b>: {selectedAdapter.description}<br/>
                    </p>
                    <p style={{ margin: "20px 20px"}}>
                      <b>Inputs to adapter: </b>
                      <Button onClick={() => this.setState({ showAdapterSpecs: true })}>
                        See Input Specs
                      </Button>
                      <Modal
                        title="Adpater Inputs Detail"
                        visible={this.state.showAdapterSpecs}
                        onOk={() => this.setState({ showAdapterSpecs: false })}
                        onCancel={() => this.setState({ showAdapterSpecs: false })}
                      >
                        <pre>
                          {/* • <b><u>Inputs</u></b>:  */}
                          {_.isEmpty(selectedAdapter.inputs) ? <p>None</p> :Object.keys(selectedAdapter.inputs).map(
                            (inputKey, idx) => (<pre key={`input-${idx}`}>
                              <b><u>{inputKey}</u></b>:<br/>
                                Type: <input value={selectedAdapter.inputs[inputKey].id} readOnly/>;<br/>
                                Optional: <input value={JSON.stringify(selectedAdapter.inputs[inputKey].optional)} readOnly/>
                            </pre>))}<br/>
                          {/* • <b><u>Outputs</u></b>: {_.isEmpty(selectedAdapter.outputs) ? <p>None</p> :Object.keys(selectedAdapter.outputs).map((outputKey, idx) => (
                            <pre key={`input-${idx}`}>
                              <b><u>{outputKey}</u></b>:<br/>
                                Type: <input value={selectedAdapter.outputs[outputKey].id} readOnly/>;<br/>
                                Optional: <input value={JSON.stringify(selectedAdapter.outputs[outputKey].optional)} readOnly/>
                            </pre>))}<br/> */}
                        </pre>                     
                      </Modal>
                    </p>
                    <form>
                      {Object.keys(selectedAdapter.inputs).filter(
                        ip => !selectedAdapter.inputs[ip].optional
                      ).map((ip, idx) => {
                        return this.createNodeInput(selectedAdapter, ip, idx, false)
                      })}
                      {Object.keys(selectedAdapter.inputs).filter(
                        ip => selectedAdapter.inputs[ip].optional
                      ).map((ip, idx) => {
                        return this.createNodeInput(selectedAdapter, ip, idx, true)
                      })}
                    </form>
                    <p style={{ margin: "20px 20px"}}><b>Wiring of this adapter:</b></p>
                    {this.state.graphEdges.filter(e => e.source === this.state.selected || e.target === this.state.selected)
                    .map((e, idx) => {
                      return (<p style={{ margin: "20px 20px"}} key={`edge-${idx}`}>
                        • {`${e.source}`}.{e.output} <b>=></b> {`${e.target}`}.{e.input}
                      </p>);
                    })}
                  </React.Fragment>
                  : null}
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
