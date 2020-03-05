import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
// import {
//   INode
// } from "react-digraph";
// import { AdapterType } from "../store/AdapterStore";
import { Select, Row, Col } from 'antd';
import _ from "lodash";

const { Option } = Select;

interface InputSelectProps {
  // selectedNode: INode | null,
  // graphNodes: INode[],
  // setGraphNodes: (nodes: INode[]) => any,
  // input: string,
  // referredAdapter: AdapterType
  selectType: string,
}

interface InputSelectState {
  // FIXME: hardcode all option for this specific adapter
  input1Selected: string
}

@inject((stores: IStore) => ({
  // selectedNode: stores.pipelineStore.selectedNode,
  // graphNodes: stores.pipelineStore.graphNodes,
  // setGraphNodes: stores.pipelineStore.setGraphNodes,
}))
@observer
export class InputSelectComponent extends React.Component<
  InputSelectProps,
  InputSelectState
> {
  state: InputSelectState = {
    // hovered: false
    input1Selected: ""
  }

  handleInput1Change1 = () => {
    console.log("Should set graph node content!")
  }

  handleInput1Change2 = () => {
    console.log("Should set graph node content!")
  }

  handleInput2Change = () => {
    console.log("Should set graph node content!")
  }

  render() {
    const { selectType } = this.props;
    if (selectType === "input1") {
      return (<Row>
        <Col span={4}>
          <Select defaultValue="lucy" style={{ width: 120 }} onChange={this.handleInput1Change1}>
            <Option value="jack">Jack</Option>
            <Option value="lucy">Lucy</Option>
            <Option value="disabled" disabled>
              Disabled
            </Option>
            <Option value="Yiminghe">yiminghe</Option>
          </Select>
        </Col>
        <Col span={4}>
          {_.isEmpty(this.state.input1Selected) ? null : <Select defaultValue="lucy" style={{ width: 120 }} onChange={this.handleInput1Change2}>
            <Option value="jack">Jack</Option>
            <Option value="lucy">Lucy</Option>
            <Option value="disabled" disabled>
              Disabled
            </Option>
            <Option value="Yiminghe">yiminghe</Option>
          </Select>}
        </Col>
      </Row>);
    } else if (selectType === "input2") {
      return (<Select defaultValue="lucy" style={{ width: 120 }} onChange={this.handleInput2Change}>
        <Option value="jack">Jack</Option>
        <Option value="lucy">Lucy</Option>
        <Option value="disabled" disabled>
          Disabled
        </Option>
        <Option value="Yiminghe">yiminghe</Option>
      </Select>);
    }
    return null;
  }
}
export default InputSelectComponent;