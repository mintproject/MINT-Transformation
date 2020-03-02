import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
// import { Button, Modal } from "antd";
import {
  INode, IEdge,
} from "react-digraph";
import { AdapterType } from "../store/AdapterStore";

interface EdgeDetailProps {
  selectedEdge: IEdge,
  graphNodes: INode[],
  adapters: AdapterType[]
}

interface EdgeDetailState {
  showAdapterSpecs: boolean,
}

@inject((stores: IStore) => ({
  selectedEdge: stores.pipelineStore.selectedEdge,
  graphNodes: stores.pipelineStore.graphNodes,
  adapters: stores.adapterStore.adapters
}))
@observer
export class EdgeDetailComponent extends React.Component<
  EdgeDetailProps,
  EdgeDetailState
> {
  render() {
    const { selectedEdge, graphNodes, adapters } = this.props;
    if (selectedEdge === null) {
      return null;
    }
    const { source, target, input, output } = selectedEdge;
    const sourceAdapter = graphNodes.filter(n => n.id === source)[0].adapter;
    const targetAdapter = graphNodes.filter(n => n.id === target)[0].adapter;
    const referredSourceAd = adapters.filter(ad => ad.id === sourceAdapter.id)[0];
    const referredTargetAd = adapters.filter(ad => ad.id === targetAdapter.id)[0];

    return (<React.Fragment>
      <p style={{ margin: "20px 20px", fontSize: "1em" }}>
        • <b><u>From Adapter</u></b>: {referredSourceAd.friendly_name ? referredSourceAd.friendly_name : referredSourceAd.id}<br/><br/><br/>
        • <b><u>Output</u></b>: {output}<br/><br/><br/>
        • <b><u>To Adapter</u></b>:{referredTargetAd.friendly_name ? referredTargetAd.friendly_name : referredTargetAd.id}<br/><br/><br/>
        • <b><u>Input</u></b>: {input}<br/>
      </p>
    </React.Fragment>);
  }
}

export default EdgeDetailComponent;