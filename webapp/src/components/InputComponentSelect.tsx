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
  groupBySelected: string
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
    groupBySelected: ""
  }

  handleGroupBySelectChange = () => {
    console.log("Should set graph node content!")
  }

  handleGroupByValueChange = () => {
    console.log("Should set graph node content!")
  }

  handleAggregationChange = () => {
    console.log("Should set graph node content!")
  }

  render() {
    const { selectType } = this.props;
    if (selectType === "groupby") {
      return (<Row>
        <Col span={4}>
          <Select defaultValue="lucy" style={{ width: 120 }} onChange={this.handleGroupBySelectChange}>
            <Option value="jack">Jack</Option>
            <Option value="lucy">Lucy</Option>
            <Option value="disabled" disabled>
              Disabled
            </Option>
            <Option value="Yiminghe">yiminghe</Option>
          </Select>
        </Col>
        <Col span={4}>
          {_.isEmpty(this.state.groupBySelected) ? null : <Select defaultValue="lucy" style={{ width: 120 }} onChange={this.handleGroupByValueChange}>
            <Option value="jack">Jack</Option>
            <Option value="lucy">Lucy</Option>
            <Option value="disabled" disabled>
              Disabled
            </Option>
            <Option value="Yiminghe">yiminghe</Option>
          </Select>}
        </Col>
      </Row>);
    } else if (selectType === "aggregation") {
      return (<Select defaultValue="lucy" style={{ width: 120 }} onChange={this.handleAggregationChange}>
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