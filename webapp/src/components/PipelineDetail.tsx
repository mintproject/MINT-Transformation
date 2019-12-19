import React from "react";
import { observer, inject } from "mobx-react";
import { IStore, AppStore } from "../store";
import { PipelineType } from "../store/AppStore";
import { RouteComponentProps } from "react-router";
import MyLayout from "./Layout";
import { Card } from "antd";
import _ from "lodash";


type PipelineProps = {
  currentPipeline: PipelineType,
  getPipeline: (pipelineId: string) => any,
}
interface PipelineState {}

@inject((stores: IStore) => ({
  currentPipeline: stores.app.currentPipeline,
  getPipeline: stores.app.getPipeline
}))
@observer
export class PipelineComponent extends React.Component<
  PipelineProps & RouteComponentProps,
  PipelineState
> {
  public state: PipelineState = {};

  componentDidMount () {
    // @ts-ignore
    this.props.getPipeline(this.props.match.params.pipelineId)
  }

  render() {
    const { currentPipeline } = this.props;
    if (currentPipeline === null) {
        return <MyLayout></MyLayout>;
    }
    return (
      <MyLayout>
        <Card
          title={currentPipeline.name}
          bordered={true}
          style={{ margin: "5px 5px" }}
        >
          {Object.keys(currentPipeline).map((k, index) => (
            <p key={`row-${index}`}>
              • <b><u>{k}</u></b>: {_.get(currentPipeline, k)}<br/>
            </p>
          ))}
        </Card>
      </MyLayout>
    );
  }
}

export default PipelineComponent;