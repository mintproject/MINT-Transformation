import React from "react";
import {inject, observer} from "mobx-react";
import {IStore} from "../store";
import {Button, Modal} from "antd";
import "antd/dist/antd.css";
import {AdapterType} from "../store/AdapterStore";
import {IEdge, INode,} from "react-digraph";
import _ from "lodash";
import InputWiredComponent from "./InputComponentWired";
import InputTextComponent from "./InputComponentText";
import InputFilePathComponent from "./InputComponentFilePath";

interface AdapterInputsProps {
  selectedNode: INode | null,
  graphNodes: INode[],
  graphEdges: IEdge[],
  setGraphNodes: (nodes: INode[]) => any,
  setGraphEdges: (edges: IEdge[]) => any,
  setSelectedNode: (node: INode | null) => any,
  adapters: AdapterType[],
  graphCreated: boolean,
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
  graphCreated: stores.pipelineStore.graphCreated,
}))
@observer
export class AdapterInputsComponent extends React.Component<
  AdapterInputsProps,
  AdapterInputsState
> {
  public state: AdapterInputsState = {
    showAdapterSpecs: false,
  }

  componentDidUpdate(prevProps: AdapterInputsProps) {
    if(prevProps.graphCreated === false && this.props.graphCreated === true) {
      this.setState({
        showAdapterSpecs: false
      })
    }
  }

  createNodeInput = (referredAdapter: AdapterType, selectedNode: INode, ip: string, idx: number, optional: boolean) => {
    const { graphEdges } = this.props;
    const selectedAdapter = selectedNode.adapter;
    if (selectedAdapter.inputs[ip].id === "dataset") {
      const wiredEdges = graphEdges.filter(e => e.target === selectedNode.id && e.input === ip);
      // this should be a dropdown to select from
      return <div key={`input-${idx}`} style={{ margin: "20px 20px"}}>
        {`${ip}: `}
        {!optional ? <span style={{ color: "red" }}>{`*`}</span> : null}
        <InputWiredComponent
          selectedNode={selectedNode}
          graphNodes={this.props.graphNodes}
          graphEdges={this.props.graphEdges}
          setGraphEdges={this.props.setGraphEdges}
          setSelectedNode={this.props.setSelectedNode}
          input={ip}
          wiredEdges={wiredEdges}
        />
      </div>
    } else if (selectedAdapter.inputs[ip].id === "file_path") {
      return (
        <div key={`input-${idx}`} style={{ margin: "20px 20px"}}>
          {`${ip}: `}
          {!optional ? <span style={{ color: "red" }}>{`*`}</span> : null}
          <InputFilePathComponent
            selectedNode={selectedNode}
            graphNodes={this.props.graphNodes}
            setGraphNodes={this.props.setGraphNodes}
            input={ip}
            referredAdapter={referredAdapter}
          />
        </div>
      );
    }
    else {
      return (
        <div key={`input-${idx}`} style={{ margin: "20px 20px"}}>
          {`${ip}: `}
          {!optional ? <span style={{ color: "red" }}>{`*`}</span> : null}
          <InputTextComponent
            selectedNode={selectedNode}
            graphNodes={this.props.graphNodes}
            setGraphNodes={this.props.setGraphNodes}
            input={ip}
            referredAdapter={referredAdapter}
          />
        </div>
      );
    }
  }

  render() {
    const { selectedNode, graphEdges, adapters } = this.props;
    const selectedAdapter = selectedNode ? selectedNode.adapter : null;
    if (selectedAdapter === null) {
      return null;
    }
    const referredAdapter = adapters.filter(a => a.id === selectedAdapter.id)[0];
    return ( selectedNode === null ? null : <React.Fragment>
      <p style={{ margin: "20px 20px"}}>
        • <b><u>Function Friendly Name</u></b>: {referredAdapter.friendly_name}<br/>
        • <b><u>Function Type</u></b>: {referredAdapter.func_type}<br/>
        • <b><u>Description</u></b>: {referredAdapter.description}<br/>
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
            {_.isEmpty(referredAdapter.inputs) ? <p>None</p> :Object.keys(referredAdapter.inputs).map(
              (inputKey, idx) => (<pre key={`input-${idx}`}>
                <b><u>{inputKey}</u></b>:<br/>
                  Type: <input value={referredAdapter.inputs[inputKey].id} readOnly/>;<br/>
                  Optional: <input value={JSON.stringify(referredAdapter.inputs[inputKey].optional)} readOnly/>
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
        {Object.keys(referredAdapter.inputs).filter(
          ip => !referredAdapter.inputs[ip].optional
        ).map((ip, idx) => {
          return this.createNodeInput(referredAdapter, selectedNode, ip, idx, false)
        })}
        {Object.keys(referredAdapter.inputs).filter(
          ip => referredAdapter.inputs[ip].optional
        ).map((ip, idx) => {
          return this.createNodeInput(referredAdapter, selectedNode, ip, idx, true)
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