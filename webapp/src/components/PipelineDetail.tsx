import React from "react";
import {inject, observer} from "mobx-react";
import {IStore} from "../store";
import {PipelineType} from "../store/PipelineStore";
import {RouteComponentProps} from "react-router";
import MyLayout from "./Layout";
import {Alert, Button, Card, Col, Input, Row, Spin} from "antd";
import _ from "lodash";

const { TextArea } = Input;


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
          </Spin> :
          <React.Fragment>
            <Col span={12}>
              <Card
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
                      onClick={() => this.setState({ openConfig: true, openOutput: false })}>
                      Click Me To View
                    </Button>
                    </React.Fragment><br/><br/>
                  • <b><u>Output</u></b>: {currentPipeline.output ? <React.Fragment>
                    <Button
                      type="primary" size="small"
                      onClick={() => this.setState({ openOutput: true, openConfig: false })}>
                      Click Me To View
                    </Button>
                    </React.Fragment> : `None`}<br/><br/>
                </pre>
              </Card>
            </Col>
          <Col span={12}>
            {this.state.openConfig ? <React.Fragment>
                <Row>
                <TextArea
                  autosize
                  title="Config" wrap="hard"
                  value={JSON.stringify(currentPipeline.config, null, 2)}
                >
                </TextArea>
              </Row>
              <Row>
                <Button
                  style={{ margin: "10px", float: "right" }}
                  onClick={() => this.setState({ openConfig: false })}>
                  Cancel
                </Button>
              </Row>
            </React.Fragment> : null}
            {this.state.openOutput ? <React.Fragment>
                <Row>
                <TextArea
                  autosize
                  title="Output" wrap="hard"
                  value={currentPipeline.output ? currentPipeline.output.replace("\n", "&#10;") : ""}
                >
                </TextArea>
              </Row>
              <Row>
                <Button
                  style={{ margin: "10px", float: "right" }}
                  onClick={() => this.setState({ openOutput: false })}>
                  Cancel
                </Button>
              </Row>
            </React.Fragment> : null}
          </Col>
          </React.Fragment>
        }
      </MyLayout>
    );
  }
}

export default PipelineComponent;