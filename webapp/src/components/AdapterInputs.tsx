import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { Button, Modal } from "antd";
import "antd/dist/antd.css";
import { AdapterType } from "../store/AdapterStore";
import {
  INode, IEdge,
} from "react-digraph";
import _ from "lodash";

interface AdapterInputsProps {
  selectedNode: INode | null,
  graphNodes: INode[],
  graphEdges: IEdge[],
  setGraphNodes: (nodes: INode[]) => any,
  setGraphEdges: (edges: IEdge[]) => any,
  setSelectedNode: (node: INode | null) => any,
  adapters: AdapterType[],
}

interface AdapterInputsState {
  showAdapterSpecs: boolean,
}

@inject((stores: IStore) => ({
  selectedNode: stores.pipelineStore.selectedNode,
  graphNodes: stores.pipelineStore.graphNodes,
  graphEdges: stores.pipelineStore.graphEdges,
  setGraphNodes: stores.pipelineStore.setGraphNodes,
  setGraphEdges: stores.pipelineStore.setGraphEdges,
  setSelectedNode: stores.pipelineStore.setSelectedNode,
  adapters: stores.adapterStore.adapters,
}))
@observer
export class AdapterInputsComponent extends React.Component<
  AdapterInputsProps,
  AdapterInputsState
> {
  public state: AdapterInputsState = {
    showAdapterSpecs: false,
  }

  createNodeInput = (selectedNode: INode, ip: string, idx: number, optional: boolean) => {
    const { graphNodes, graphEdges, setGraphNodes } = this.props;
    const selectedAdapter = selectedNode.adapter;
    if (selectedAdapter.inputs[ip].id === "graph") {
      const wiredEdges = graphEdges.filter(e => e.target === selectedNode.id && e.input === "graph");
      // this should be a dropdown to select from
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
          value={selectedAdapter.inputs[ip].val || ""}
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
        />
      </p>
    );
  }

  render() {
    const { selectedNode, graphEdges } = this.props;
    const selectedAdapter = selectedNode ? selectedNode.adapter : null;
    return ( selectedNode === null ? null : <React.Fragment>
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
            {/* • <b><u>Outputs</u></b>: {_.isEmpty(selectedNode.outputs) ? <p>None</p> :Object.keys(selectedNode.outputs).map((outputKey, idx) => (
              <pre key={`input-${idx}`}>
                <b><u>{outputKey}</u></b>:<br/>
                  Type: <input value={selectedNode.outputs[outputKey].id} readOnly/>;<br/>
                  Optional: <input value={JSON.stringify(selectedNode.outputs[outputKey].optional)} readOnly/>
              </pre>))}<br/> */}
          </pre>                     
        </Modal>
      </p>
      <form>
        {Object.keys(selectedAdapter.inputs).filter(
          ip => !selectedAdapter.inputs[ip].optional
        ).map((ip, idx) => {
          return this.createNodeInput(selectedNode, ip, idx, false)
        })}
        {Object.keys(selectedAdapter.inputs).filter(
          ip => selectedAdapter.inputs[ip].optional
        ).map((ip, idx) => {
          return this.createNodeInput(selectedNode, ip, idx, true)
        })}
      </form>
      <p style={{ margin: "20px 20px"}}><b>Wiring of this adapter:</b></p>
      {graphEdges.filter(e => e.source === selectedNode.id || e.target === selectedNode.id)
      .map((e, idx) => {
        return (<p style={{ margin: "20px 20px"}} key={`edge-${idx}`}>
          • {`${e.source}`}.{e.output} <b>=></b> {`${e.target}`}.{e.input}
        </p>);
      })}
    </React.Fragment>);
  }
}

export default AdapterInputsComponent;