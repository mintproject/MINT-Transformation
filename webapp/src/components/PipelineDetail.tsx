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
              {/* {JSON.stringify(currentPipeline, undefined, 2)} */}
              {/*{
                "config": {
                  "adapters": {
                    "my_read_func": {
                      "adapter": "funcs.ReadFunc",
                      "inputs": {
                        "repr_file": "./wfp_food_prices_south-sudan.repr.yml",
                        "resources": "./wfp_food_prices_south-sudan.csv"
                      }
                    }
                  },
                  "description": "Test description",
                  "version": "1"
                },
                "description": "Test description",
                "end_timestamp": "2020-01-15T18:27:45",
                "id": "48b62deeb48d",
                "name": "Test Name",
                "output": "Importing TopoFlow 3.6 packages:\n   topoflow.utils\n   topoflow.utils.tests\n   topoflow.components\n   topoflow.components.tests\n   topoflow.framework\n   topoflow.framework.tests\n \nImporting TopoFlow 3.6 packages:\n   topoflow.utils\n   topoflow.utils.tests\n   topoflow.components\n   topoflow.components.tests\n   topoflow.framework\n   topoflow.framework.tests\n \nPaths for this package:\nframework_dir = /ws/extra_libs/topoflow/framework/\nparent_dir    = /ws/extra_libs/topoflow/\nexamples_dir  = /ws/extra_libs/topoflow/examples/\n__file__      = /ws/extra_libs/topoflow/framework/emeli.py\n__name__      = topoflow.framework.emeli\n \n",
                "start_timestamp": "2020-01-15T18:27:42",
                "status": "finished"
              } */}
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