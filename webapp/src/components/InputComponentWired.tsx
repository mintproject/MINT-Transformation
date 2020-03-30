import React from "react";
import {inject, observer} from "mobx-react";
import {IStore} from "../store";
import {Dropdown, Input, Menu} from "antd";
import {IEdge, INode,} from "react-digraph";
import _ from "lodash";

interface InputWiredProps {
  selectedNode: INode | null,
  graphNodes: INode[],
  graphEdges: IEdge[],
  setGraphEdges: (edges: IEdge[]) => any,
  setSelectedNode: (node: INode | null) => any,
  input: string,
  wiredEdges: IEdge[],
}

interface InputWiredState {}

@inject((stores: IStore) => ({
  selectedNode: stores.pipelineStore.selectedNode,
  graphNodes: stores.pipelineStore.graphNodes,
  graphEdges: stores.pipelineStore.graphEdges,
  setGraphEdges: stores.pipelineStore.setGraphEdges,
  setSelectedNode: stores.pipelineStore.setSelectedNode,
}))
@observer
export class InputWiredComponent extends React.Component<
  InputWiredProps,
  InputWiredState
> {
  createEdgeMenu = () => {
    const { graphNodes, selectedNode, setGraphEdges, input, wiredEdges } = this.props;
    const possibleNodes = graphNodes.filter(n => n.id !== selectedNode!.id);
    var menuList = [];
    for (let i = 0; i < possibleNodes.length; i ++) {
      let outputs = possibleNodes[i].adapter.outputs;
      let outputsMenuList = Object.keys(outputs).map(o => ({
        node: possibleNodes[i].id,
        output: o
      }))
      menuList.push(...outputsMenuList);
    }
    return <Menu onClick={({ item }) => {
      const { eventKey } = item.props;
      const data = eventKey.split("-");
      let newEdges = this.props.graphEdges;
      if (data[1] === "null" && data[2] === "null") {
        let oldEdges = newEdges.filter(e => (e.target === selectedNode!.id && e.input === input));
        if (oldEdges.length === 0) { return; }
        let oldEdge = oldEdges[0];
        newEdges = newEdges.filter(e => !(e.target === selectedNode!.id && e.input === input));
        let sameSourceSameTarget = newEdges.filter(
          ed => ed.source === oldEdge.source && ed.target === oldEdge.target
        );
        let newEdgeType = "emptyEdge";
        if (sameSourceSameTarget.length > 1) {
          console.log("Adding an edge: multiEdge!")
          newEdgeType = "multiEdge";
        }
        sameSourceSameTarget.forEach(ed => {
          ed.type = newEdgeType
        })
      } else {
        if (wiredEdges.length > 0) {
          // remove old wiring
          newEdges = newEdges.filter(e => !wiredEdges.includes(e))
        }
        let sameSourceSameTarget = newEdges.filter(
          ed => ed.source === data[1] && ed.target === selectedNode!.id
        );
        let newEdgeType = "emptyEdge";
        if (sameSourceSameTarget.length > 0) {
          console.log("Adding an edge: multiEdge!")
          newEdgeType = "multiEdge";
        }
        sameSourceSameTarget.forEach(ed => {
          ed.type = newEdgeType
        })
        newEdges.push({
          target: selectedNode!.id,
          source: data[1],
          input: input,
          output: data[2],
          type: newEdgeType
        });
      }
      setGraphEdges(newEdges);
      }}>
      <Menu.Item key={`menu-null-null-0`}>
        {`None`}
      </Menu.Item>
      {menuList.map((m, idx) => {
        return (
          <Menu.Item key={`menu-${m.node}-${m.output}-${idx}`}>
            {`Node ${m.node} - output name: ${m.output}`}
          </Menu.Item>
        );
      })}      
    </Menu>
  }

  render() {
    const { wiredEdges } = this.props;
    const menu = this.createEdgeMenu(); 
    return (
      <Dropdown overlay={menu}>
        <Input
          disabled={true}
          type="text"
          value={
            _.isEmpty(wiredEdges) ? "Select from possible outputs..."
            : `${wiredEdges[0].source}.${wiredEdges[0].output}`
          }
          style={{
            padding:"5px",
            border:"2px solid",
            borderRadius: "5px",
            width: "100%"
          }}
        />
      </Dropdown>
    );
  }
}

export default InputWiredComponent;