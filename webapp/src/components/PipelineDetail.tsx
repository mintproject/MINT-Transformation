import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { PipelineType } from "../store/PipelineStore";
import { RouteComponentProps } from "react-router";
import MyLayout from "./Layout";
import { Card, Alert, Spin } from "antd";
import _ from "lodash";


type PipelineProps = {
  currentPipeline: PipelineType,
  getPipeline: (pipelineId: string) => any,
}
interface PipelineState {}

@inject((stores: IStore) => ({
  currentPipeline: stores.pipelineStore.currentPipeline,
  getPipeline: stores.pipelineStore.getPipeline
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
        { _.isEmpty(this.props.currentPipeline) ?
          <Spin size="large" style={{ textAlign: "center" }}>
            <Alert
              message="Loading pipeline details"
              description="Right now is really slow :/"
              type="info"
            />
          </Spin> : <Card
            title={currentPipeline.name}
            bordered={true}
            style={{ margin: "5px 5px" }}
          >
            {/* {Object.keys(currentPipeline).map((k, index) => (
              <p key={`row-${index}`}>
                • <b><u>{k}</u></b>: {_.get(currentPipeline, k)}<br/>
              </p>
            ))} */}
            <pre>
              {JSON.stringify(currentPipeline, undefined, 2)}
            </pre>
          </Card>
        }
      </MyLayout>
    );
  }
}

export default PipelineComponent;