import React from "react";
import {inject, observer} from "mobx-react";
import {IStore} from "../store";
import { Card } from "antd";
import {IEdge, INode,} from "react-digraph";
import {AdapterType} from "../store/AdapterStore";

interface EdgeDetailProps {
  selectedEdge: IEdge,
  graphNodes: INode[],
  graphEdges: IEdge[],
  adapters: AdapterType[]
}

interface EdgeDetailState {
  showAdapterSpecs: boolean,
}

@inject((stores: IStore) => ({
  selectedEdge: stores.pipelineStore.selectedEdge,
  graphNodes: stores.pipelineStore.graphNodes,
  graphEdges: stores.pipelineStore.graphEdges,
  adapters: stores.adapterStore.adapters
}))
@observer
export class EdgeDetailComponent extends React.Component<
  EdgeDetailProps,
  EdgeDetailState
> {
  render() {
    const { selectedEdge, graphNodes, adapters, graphEdges } = this.props;
    if (selectedEdge === null) {
      return null;
    }
    const { source, target, input, output } = selectedEdge;
    const selectedEdges = graphEdges.filter(ed => ed.source === source && ed.target === target);

    console.log("HELP")
    console.log(selectedEdges)

    return (<React.Fragment>
      {selectedEdges.map((ed, idx) => {
        const sourceAdapter = graphNodes.filter(n => n.id === ed.source)[0].adapter;
        const targetAdapter = graphNodes.filter(n => n.id === ed.target)[0].adapter;
        const referredSourceAd = adapters.filter(ad => ad.id === sourceAdapter.id)[0];
        const referredTargetAd = adapters.filter(ad => ad.id === targetAdapter.id)[0];

        return (
          <Card
            title={`Edge # ${idx}`}
            bordered={true}
            style={{ margin: "10px 10px" }}
            key={`card-${idx}`}
            hoverable
          >
            <p style={{ margin: "20px 20px", fontSize: "1em" }}>
              • <b><u>From Adapter</u></b>: {referredSourceAd.friendly_name ? referredSourceAd.friendly_name : referredSourceAd.id}<br/><br/><br/>
              • <b><u>Output</u></b>: {output}<br/><br/><br/>
              • <b><u>To Adapter</u></b>:{referredTargetAd.friendly_name ? referredTargetAd.friendly_name : referredTargetAd.id}<br/><br/><br/>
              • <b><u>Input</u></b>: {input}<br/>
            </p>
          </Card>
        );
      })}
    </React.Fragment>)
  }
}

export default EdgeDetailComponent;