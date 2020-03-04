import React from "react";
import {inject, observer} from "mobx-react";
import {IStore} from "../store";
import {INode} from "react-digraph";
import {AdapterType} from "../store/AdapterStore";

interface InputTextProps {
  selectedNode: INode | null,
  graphNodes: INode[],
  setGraphNodes: (nodes: INode[]) => any,
  input: string,
  referredAdapter: AdapterType
}

interface InputTextState {
  hovered: boolean
}

@inject((stores: IStore) => ({
  selectedNode: stores.pipelineStore.selectedNode,
  graphNodes: stores.pipelineStore.graphNodes,
  setGraphNodes: stores.pipelineStore.setGraphNodes,
}))
@observer
export class InputTextComponent extends React.Component<
  InputTextProps,
  InputTextState
> {
  state: InputTextState = {
    hovered: false
  }

  render() {
    const { selectedNode, input, graphNodes, setGraphNodes, referredAdapter } = this.props;
    if (selectedNode === null) { return; }
    const selectedAdapter = selectedNode!.adapter;
    return (
      <p
        onMouseEnter={() => this.setState({ hovered: true })}
        onMouseLeave={() => this.setState({ hovered: false })}
      >
      <textarea
        name={input}
        value={selectedAdapter.inputs[input].val || ""}
        onChange={(event: React.ChangeEvent<HTMLTextAreaElement>) => {
          const { currentTarget } = event;
          var newNodes = graphNodes;
          var newNode = newNodes.filter(n => n.id === selectedNode.id)[0];
          newNode.adapter.inputs[currentTarget.name].val = currentTarget.value;
          // this.setState({ graphNodes: newNodes });
          setGraphNodes(newNodes);
        }}
        wrap="soft"
        style={{
          padding:"5px",
          border:"2px solid",
          borderRadius: "5px",
          width: "100%"
        }}
      />
      {this.state.hovered ? <span style={{ color: "red" }}>
        {`Hint: Please enter a valid string, for instance: "${referredAdapter.example[input]}"`}
      </span> : null}
      </p>
    );
  }
}

export default InputTextComponent;