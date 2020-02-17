import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { Menu, Dropdown, Input } from "antd";
import {
  INode, IEdge,
} from "react-digraph";
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
    const { graphNodes, graphEdges, selectedNode, setGraphEdges, input, wiredEdges } = this.props;
    const possibleNodes = graphNodes.filter(n => n.id !== selectedNode!.id);
    var menuList = [];
    for (var i = 0; i < possibleNodes.length; i ++) {
      var outputs = possibleNodes[i].adapter.outputs;
      var outputsMenuList = Object.keys(outputs).map(o => ({
        node: possibleNodes[i].id,
        output: o
      }))
      menuList.push(...outputsMenuList);
    }
    return <Menu onClick={({ item }) => {
      const { eventKey } = item.props;
      const data = eventKey.split("-");
      var newEdges = graphEdges
      if (data[1] === "null" && data[2] === "null") {
        newEdges = newEdges.filter(e => !(e.target === selectedNode!.id && e.input === input))
      } else {
        if (wiredEdges.length > 0) {
          // remove old wiring
          newEdges = newEdges.filter(e => !wiredEdges.includes(e))
        }
        newEdges.push({
          target: selectedNode!.id,
          source: data[1],
          input: input,
          output: data[2]
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