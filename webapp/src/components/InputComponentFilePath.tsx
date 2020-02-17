import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import {
  INode
} from "react-digraph";
import _ from "lodash";

interface InputFilePathProps {
  selectedNode: INode | null,
  graphNodes: INode[],
  setGraphNodes: (nodes: INode[]) => any,
  input: string,
}

interface InputFilePathState {
  hovered: boolean
}

@inject((stores: IStore) => ({
  selectedNode: stores.pipelineStore.selectedNode,
  graphNodes: stores.pipelineStore.graphNodes,
  setGraphNodes: stores.pipelineStore.setGraphNodes,
}))
@observer
export class InputFilePathComponent extends React.Component<
  InputFilePathProps,
  InputFilePathState
> {
  state: InputFilePathState = {
    hovered: false
  }

  render() {
    const { selectedNode, input, graphNodes, setGraphNodes } = this.props;
    if (selectedNode === null) { return; }
    const selectedAdapter = selectedNode!.adapter;
    return (
      <React.Fragment>
        <input
          name={input}
          value={selectedAdapter.inputs[input].val || ""}
          onChange={(event: React.ChangeEvent<HTMLInputElement>) => {
            const { currentTarget } = event;
            var newNodes = graphNodes;
            var newNode = newNodes.filter(n => n.id === selectedNode.id)[0];
            newNode.adapter.inputs[currentTarget.name].val = currentTarget.value;
            // this.setState({ graphNodes: newNodes });
            setGraphNodes(newNodes);
          }}
          style={{
            padding:"5px",
            border:"2px solid",
            borderRadius: "5px",
            width: "100%"
          }}
          onMouseEnter={() => this.setState({ hovered: true })}
          onMouseLeave={() => this.setState({ hovered: false })}
        />
        { this.state.hovered ? <span style={{ color: "red" }}>
          {`Hint: Please enter a valid file path, for instance: "./wfp_food_prices_south-sudan.repr.yml"`}
        </span> : null}
      </React.Fragment>
    );
  }
}

export default InputFilePathComponent;