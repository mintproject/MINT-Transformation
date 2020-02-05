import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { PipelineType } from "../store/PipelineStore";
import { RouteComponentProps } from "react-router";
import MyLayout from "./Layout";
import { Card, Alert, Spin, Button, Modal } from "antd";
import _ from "lodash";


type PipelineProps = {
  currentPipeline: PipelineType,
  getPipeline: (pipelineId: string) => any,
}
interface PipelineState {
  openConfig: boolean,
  openOutput: boolean,
}

@inject((stores: IStore) => ({
  currentPipeline: stores.pipelineStore.currentPipeline,
  getPipeline: stores.pipelineStore.getPipeline
}))
@observer
export class PipelineComponent extends React.Component<
  PipelineProps & RouteComponentProps,
  PipelineState
> {
  public state: PipelineState = {
    openConfig: false,
    openOutput: false,
  };

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
            style={{ margin: "5px 5px", height: "100%" }}
          >
            <pre>
              • <b><u>Pipeline ID</u></b>: {currentPipeline.id}<br/><br/>
              • <b><u>Pipeline Name</u></b>: {currentPipeline.name}<br/><br/>
              • <b><u>Description</u></b>: {currentPipeline.description}<br/><br/>
              • <b><u>Start Timestamp</u></b>: {currentPipeline.start_timestamp}<br/><br/>
              • <b><u>End Timestamp</u></b>: {currentPipeline.end_timestamp}<br/><br/>
              • <b><u>Status</u></b>: {currentPipeline.status}<br/><br/>
              • <b><u>Config</u></b>: <React.Fragment>
                <Button
                  type="primary" size="small"
                  onClick={() => this.setState({ openConfig: true })}>
                  Click Me To View
                </Button>
                <Modal
                  title="Config"
                  visible={this.state.openConfig}
                  onOk={() => this.setState({ openConfig: false })}
                  onCancel={() => this.setState({ openConfig: false })}
                >
                  <pre>
                    {JSON.stringify(currentPipeline.config, null, 2)}
                  </pre>
                </Modal>
                </React.Fragment><br/><br/>
              • <b><u>Output</u></b>: {currentPipeline.output ? <React.Fragment>
                <Button
                  type="primary" size="small"
                  onClick={() => this.setState({ openOutput: true })}>
                  Click Me To View
                </Button>
                <Modal
                  title="Output"
                  visible={this.state.openOutput}
                  onOk={() => this.setState({ openOutput: false })}
                  onCancel={() => this.setState({ openOutput: false })}
                >
                  <pre>
                    {currentPipeline.output.split("\n").map((op, idx) => (<p key={`output-${idx}`}>
                      {op.trim()}
                    </p>))}
                  </pre>
                </Modal>
                </React.Fragment> : `None`}<br/><br/>
            </pre>
          </Card>
        }
      </MyLayout>
    );
  }
}

export default PipelineComponent;