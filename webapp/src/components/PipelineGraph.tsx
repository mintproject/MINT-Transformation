import React from "react";
import {inject, observer} from "mobx-react";
import {IStore} from "../store";
import {Button, Dropdown, Input, Menu, Modal, Row} from "antd";
import "antd/dist/antd.css";
import {AdapterType} from "../store/AdapterStore";
import {GraphView, IEdge, INode,} from "react-digraph";
import _ from "lodash";
import SubMenu from "antd/lib/menu/SubMenu";

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

interface PipelineGraphProps {
  selectedNode: INode | null,
  graphNodes: INode[],
  graphEdges: IEdge[],
  setGraphNodes: (nodes: INode[]) => any,
  setGraphEdges: (edges: IEdge[]) => any,
  setSelectedNode: (node: INode | null) => any,
  setSelectedEdge: (edge: IEdge | null) => any,
  adapters: AdapterType[],
  graphCreated: boolean,
}
interface PipelineGraphState {
  currentAction: string,
  currentAdapter: string,
  currentAdapterName: string,
  currentToNode: string,
  fromNodeOutput: string,
  toNodeInput: string,
  nameAdapter: boolean,
}

@inject((stores: IStore) => ({
  selectedNode: stores.pipelineStore.selectedNode,
  graphNodes: stores.pipelineStore.graphNodes,
  graphEdges: stores.pipelineStore.graphEdges,
  setGraphNodes: stores.pipelineStore.setGraphNodes,
  setGraphEdges: stores.pipelineStore.setGraphEdges,
  setSelectedNode: stores.pipelineStore.setSelectedNode,
  setSelectedEdge: stores.pipelineStore.setSelectedEdge,
  adapters: stores.adapterStore.adapters,
  graphCreated: stores.pipelineStore.graphCreated,
}))
@observer
export class PipelineGraphComponent extends React.Component<
  PipelineGraphProps,
  PipelineGraphState
> {
  public state: PipelineGraphState = {
    currentAction: "",
    currentAdapter: "",
    currentAdapterName: "",
    currentToNode: "",
    fromNodeOutput: "",
    toNodeInput: "",
    nameAdapter: false,
  }

  componentDidUpdate(prevProps: PipelineGraphProps) {
    if(prevProps.graphCreated === false && this.props.graphCreated === true) {
      this.setState({
        currentAction: "",
        currentAdapter: "",
        currentAdapterName: "",
        currentToNode: "",
        fromNodeOutput: "",
        toNodeInput: "",
        nameAdapter: false,
      })
    }
  }

  onUpdateNode = (node: INode) => {
    const { graphNodes, setGraphNodes, setSelectedNode } = this.props;
    const selectedNode = graphNodes.filter(n => n.id === node.id)[0];
    console.log(selectedNode);
    setSelectedNode(selectedNode);
    setGraphNodes(graphNodes.map(v => v.id === node.id ? node : v));
  };
  
  createAdapterMenu = () => {
    const currAdapters = this.props.adapters;
    const currAdapterTypes = currAdapters.map(ad => ad.func_type).filter(
      (v, i, a) => a.indexOf(v) === i
    );
    const nonTransAdapters = currAdapterTypes.filter(adt => !adt.includes("Transformation"));
    var transAdapters = currAdapterTypes.filter(adt => adt.includes("Transformation"));
    transAdapters = transAdapters.sort((a, b) => {
      if (a.includes("Other")) { return 1; }
      if (b.includes("Other")) { return -1; }
      return a > b ? 1 : -1;
    });
    return (<Menu onClick={({ item }) => {
      const currentAdapter = item.props.eventKey;
      const { maxX, maxY, minX, minY, maxId } = this.findBoundingBoxGraphNodes();
      const customName = `Adapter_${maxId + 1}`;
      const newNode: INode = {
        id: customName,
        title: `${currentAdapter}`,
        // type: "empty",
        x: 300 + maxX,
        y: (maxY + minY)/2,
        // FIXME: we need a deep copy!!!!
        // adapter: this.props.adapters.filter(ad => ad.id === currentAdapter)[0]
        adapter: JSON.parse(JSON.stringify(this.props.adapters.filter(ad => ad.id === currentAdapter)[0]))
      };
      const newNodes = this.props.graphNodes;
      newNodes.push(newNode);
      this.props.setGraphNodes(newNodes);
      this.props.setSelectedNode(null);
      this.setState({
        currentAction: "",
        currentAdapter: "",
        // currentAdapterName: "",
        // nameAdapter: false
      });
      // this.setState({
      //   currentAdapter: item.props.eventKey,
      //   // nameAdapter: true
      // });
    }}>
      {nonTransAdapters.map((adt, i) => {
        return (<SubMenu
            key={`adt-${i}`} title={adt}
          >
          {currAdapters.filter(ad => ad.func_type === adt).map((ad, idx) => {
            return (<Menu.Item key={ad.id}>
              {_.isEmpty(ad.friendly_name) ? ad.id : ad.friendly_name}
            </Menu.Item>);
          })}
        </SubMenu>)
      })}
      <Menu.ItemGroup title="Transformers">
        {transAdapters.map((adt, i) => {
          return (<SubMenu
              key={`trans-adt-${i}`} title={adt.split(" Transformation")[0]}
            >
            {currAdapters.filter(ad => ad.func_type === adt).map((ad, idx) => {
              return (<Menu.Item key={ad.id} disabled={ad.is_fake === true}>
                {_.isEmpty(ad.friendly_name) ? ad.id : ad.friendly_name}
              </Menu.Item>);
            })}
          </SubMenu>)
        })}
      </Menu.ItemGroup>
    </Menu>);
  };

  createFromNodeOutputMenu = () => {
    const selected = this.props.selectedNode ? this.props.selectedNode.id : "";
    const selectedNode = this.props.graphNodes.filter(
      n => n.id === selected
    )[0].adapter;
    return (<Menu onClick={({ item }) => {
      this.setState({ fromNodeOutput: item.props.children });
      }}>
      {Object.keys(selectedNode.outputs).map((k, idx) => {
        return (<Menu.Item key={`output-${idx}`}>
          {k}
        </Menu.Item>);
      })}
    </Menu>);
  };

  createToNodeMenu = () => {
    const selected = this.props.selectedNode ? this.props.selectedNode.id : "";
    return (<Menu onClick={({ item }) => {
      this.setState({ currentToNode: item.props.children });
      }}>
      {this.props.graphNodes.filter(
        n => n.id !== selected
      ).map((node, idx) => {
        return (<Menu.Item key={`${idx}`}>
          {node.id}
        </Menu.Item>);
      })}
    </Menu>);
  };

  createToNodeInputMenu = () => {
    const selectedNode = this.props.graphNodes.filter(n => n.id === this.state.currentToNode)[0].adapter;
    return (<Menu onClick={({ item }) => {
      this.setState({ toNodeInput: item.props.children });
      }}>
      {Object.keys(selectedNode.inputs).map((k, idx) => {
        return (<Menu.Item key={`input-${idx}`}>
          {k}
        </Menu.Item>);
      })}
    </Menu>);
  };

  createNodeEdgeMenu = () => {
    const selected = this.props.selectedNode ? this.props.selectedNode.id : "";
    const nodeEdges = this.props.graphEdges.filter(
      e => e.source === selected || e.target === selected
    );
    return (<Menu onClick={({ key }) => {
        const updates = key.split("-");
        // const selectedNode = this.props.adapters.filter(a => a.id === updates[0])[0];
        // this.props.setSelectedNode(selectedNode);
        this.setState({
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

  isAlphaNumericUnderscore = (check: string) => {
    if (check.length === 0) { return true; }
    const subStrings = check.split("_");
    for (var i = 0; i < subStrings.length; i++){
      if (subStrings[i].match(/^[0-9a-zA-Z]+$/g) === null) { return false; }
    }
    return true;
  }

  findBoundingBoxGraphNodes = () => {
    const { graphNodes } = this.props;
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

  onClear = () => {
    this.props.setSelectedNode(null);
    this.setState({
      currentAction: "",
      currentAdapter: "",
      currentToNode: "",
      toNodeInput: "",
      fromNodeOutput: "",
      currentAdapterName: ""
    });
  }

  onSelectEdge = (selectedEdge: IEdge) => {
    // const { target, source, input, output } = selectedEdge;
    this.props.setSelectedNode(null);
    this.props.setSelectedEdge(selectedEdge);
  }

  render() {
    const { graphNodes, graphEdges, selectedNode } = this.props;
    // FIXME: forced rerendering?
    console.log(`Number of edges is: ${graphEdges.length}`);
    const selected = selectedNode ? selectedNode.id : "";
    return (
      <React.Fragment>
        <Row style={{ margin: "20px 10px"}}>
          { _.isEmpty(selected) && _.isEmpty(this.state.currentAction) ?
            <p>
              <Button
                style={{ margin: "5px" }}
                onClick={() => this.setState({ currentAction: "add-a-new-node"})}
              >Add A New Adapter</Button>
            </p>
            : null
          }
          { selected && _.isEmpty(this.state.currentAction) ? <span>
            <p>{`Select an action on ${selected}: `}</p>
            {/* <Button
              style={{ margin: "5px" }}
              onClick={() => this.setState({ currentAction: "add-a-new-node"})}
            >Add A New Adapter</Button> */}
            {/* <Button
              style={{ margin: "5px" }}
              onClick={() => this.setState({ currentAction: "add-a-new-edge"})}
            >Add A New Edge</Button> */}
            <Button
              style={{ margin: "5px" }}
              onClick={() => this.setState({ currentAction: "delete-this-node"})}
            >Delete This Node</Button>
            {/* <Button
              style={{ margin: "5px" }}
              onClick={() => this.setState({ currentAction: "delete-its-edge"})}
            >Delete Its Edge</Button> */}
            <Button
              style={{ margin: "5px" }}
              onClick={() => this.props.setSelectedNode(null)}
              type="dashed"
            >Cancel</Button>
          </span> : null}
          { this.state.currentAction === "add-a-new-node"
            ? <span>
              <p>Add A New Adapter: </p>
              <Dropdown overlay={this.createAdapterMenu()}>
                { this.state.currentAdapter
                ? <b>{`Selected Adapter: ${this.state.currentAdapter}`}</b>
                : <Button><b>Select An Adapter</b></Button>}
              </Dropdown>
              {/* FIXME: no more naming adapters*/}
              <Modal
                title="Naming new adapter..."
                visible={this.state.nameAdapter}
                onOk={() => {
                  console.log("adding a new node to the graph");
                  const { maxX, maxY, minX, minY, maxId } = this.findBoundingBoxGraphNodes();
                  const customName = this.state.currentAdapterName ? this.state.currentAdapterName : `Adapter_${maxId + 1}`;
                  const newNode: INode = {
                    id: customName,
                    title: `${this.state.currentAdapter}`,
                    // type: "empty",
                    x: 300 + (maxX + minX)/2,
                    y: (maxY + minY)/2,
                    adapter: this.props.adapters.filter(ad => ad.id === this.state.currentAdapter)[0]
                  };
                  const newNodes = graphNodes;
                  newNodes.push(newNode);
                  this.props.setGraphNodes(newNodes);
                  this.props.setSelectedNode(null);
                  this.setState({
                    currentAction: "",
                    currentAdapter: "",
                    currentAdapterName: "",
                    nameAdapter: false
                  });
                }}
                okButtonProps={{ disabled: !this.isAlphaNumericUnderscore(this.state.currentAdapterName) }}
                onCancel={() => this.setState({
                  currentAction: "",
                  currentAdapter: "",
                  currentAdapterName: "",
                  nameAdapter: false
                })}
              >
                <p>
                  Enter adapter name (Please only use "_" as delimiter):
                  <Input
                    value={this.state.currentAdapterName}
                    onChange={({ target }) => this.setState({ currentAdapterName: target.value })}
                  /> 
                  <span style={{ color: "gray" }}>{`* Default name to Adapter_#`}</span>
                </p>
              </Modal>
            </span> : null
          }
          { this.state.currentAction === "add-a-new-edge"
            ? <span>
              <p>
                {`Add A New Edge From ${selected} `}
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
                    const newEdges = this.props.graphEdges;
                    newEdges.push({
                      target: this.state.currentToNode,
                      source: selected || "",
                      type: "empty",
                      input: this.state.toNodeInput,
                      output: this.state.fromNodeOutput
                    });
                    this.props.setGraphEdges(newEdges);
                    this.props.setSelectedNode(null);
                    this.setState({
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
                  const selected = this.props.selectedNode ? this.props.selectedNode.id : "";;
                  const newNodes = this.props.graphNodes.filter(n => n.id !== selected);
                  const newEdges = this.props.graphEdges.filter(e => e.target !== selected && e.source !== selected );
                  this.props.setGraphNodes(newNodes);
                  this.props.setGraphEdges(newEdges);
                  this.props.setSelectedNode(null);
                  this.setState({
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
              <p>{`Select An Edge From ${selected}:`} </p>
              <Dropdown overlay={this.createNodeEdgeMenu()}>
                { this.state.currentToNode
                ? <b>{
                  `Deleting The Edge From ${selected}.${this.state.fromNodeOutput}
                    To ${this.state.currentToNode}.${this.state.toNodeInput}`}</b>
                : <Button><b>Select Node Edges</b></Button>}
              </Dropdown>
              <Button
                style={{ float: "right", margin: "5px" }}
                type="danger"
                onClick={() => {
                  console.log("deleting this node in the graph");
                  const selected = this.props.selectedNode ? this.props.selectedNode.id : "";
                  const { graphEdges, setGraphEdges, setSelectedNode } = this.props;
                  const { currentToNode, fromNodeOutput, toNodeInput } = this.state;
                  // FIXME: should we update graphNodes as well?
                  const newEdges = graphEdges.filter(
                    e => e.target !== currentToNode || e.source !== selected
                    || e.input !== toNodeInput || e.output !== fromNodeOutput
                  );
                  setGraphEdges(newEdges);
                  setSelectedNode(null);
                  this.setState({
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
        </Row>
        <Row style={{ margin: "20px 10px", height: "100%" }}>
          <GraphView
            ref='GraphView'
            nodeKey="id"
            nodes={graphNodes}
            edges={graphEdges}
            selected={{}}
            nodeTypes={GraphConfig.NodeTypes}
            nodeSubtypes={GraphConfig.NodeSubtypes}
            edgeTypes={GraphConfig.EdgeTypes}
            onUpdateNode={this.onUpdateNode}
            onSelectNode={() => console.log("does nothing")}
            onCreateNode={() => console.log("does nothing")}
            onDeleteNode={() => console.log("does nothing")}
            // onSelectEdge={() => console.log("does nothing")}
            onSelectEdge={this.onSelectEdge}
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
            renderNode={() => {
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
        </Row>
      </React.Fragment>);
  }
}
export default PipelineGraphComponent;