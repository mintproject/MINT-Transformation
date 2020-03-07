import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import {
  INode
} from "react-digraph";
import { AdapterType } from "../store/AdapterStore";
import { Select, Row, Col, Button } from 'antd';
import _ from "lodash";

const { Option } = Select;

interface InputSelectProps {
  selectedNode: INode | null,
  // graphNodes: INode[],
  // setGraphNodes: (nodes: INode[]) => any,
  input: string,
  referredAdapter: AdapterType
  selectType: string,
}

interface InputSelectState {
  // FIXME: hardcode all option for this specific adapter
  groupByOptions: string[],
  aggregationOption: string,
  groupByValues: string[]
}

@inject((stores: IStore) => ({
  selectedNode: stores.pipelineStore.selectedNode,
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
    groupByOptions: [],
    groupByValues: [],
    aggregationOption: ""
  }

  componentDidMount() {
    const { input, selectedNode, selectType } = this.props;
    if (selectedNode === null) { return; }
    const selectedAdapter = selectedNode!.adapter;
    // console.log(selectedAdapter.inputs[input].val)
    // const inputList = JSON.parse(selectedAdapter.inputs[input].val || "")
    if (selectType === "groupby") {
      const inputList = selectedAdapter.inputs[input].val || [];
      // @ts-ignore
      const optionList = inputList.map(i => this.allKeys[i!.prop]);
      // @ts-ignore
      const valueList = inputList.map(i => i!.value);
      // console.log("resetting adapter input...");
      // console.log(inputList)
      this.setState({
        groupByOptions: optionList,
        groupByValues: valueList
      })
    } else {
      this.setState({
        aggregationOption: selectedAdapter.inputs[input].val
      })
    }
  }

  allOptions = ["time", "lat", "long", "place"]

  allValues = {
    "time": ["exact", "minute", "hour", "date", "month", "year"],
    "lat": ["exact"],
    "long": ["exact"],
    "place": ["exact"],
  }

  allKeys = {
    "mint:timestamp": "time",
    "mint:geo:lat": "lat",
    "mint:geo:long": "long",
    "mint:place": "place"
  }

  createKey = (label: string) => {
    // FIXME
    if (label === "time") { return "mint:timestamp"; }
    if (label === "lat") { return "mint:geo:lat"; }
    if (label === "long") { return "mint:geo:long"; }
    if (label === "place") { return "mint:place"; }
    return ""
  }


  handleGroupBySelectChange = (i: number) => {
    // should change groupByOptions at index i
    // substitute with ?
    console.log(i)
  }

  handleGroupByValueChange = () => {
    console.log("Should set graph node content!")
  }

  handleAggregationChange = (value: string) => {
    console.log(value)
  }

  renderGroupBySelection = () => {
    const { groupByOptions } = this.state;
    console.log("AM i here?")
    const availableOption = this.allOptions.filter(op => !this.state.groupByOptions.includes(op));
    console.log(this.state.groupByOptions);
    console.log(availableOption)
    return (groupByOptions.map((op, i) => { return (<Row key={`row-${op}-${i}`}>
      <Col span={8}>
        <Select style={{ width: "100px", margin: "10px" }} onChange={() => this.handleGroupBySelectChange(i)} value={op}>
          {availableOption.map((o, idx) => {
            return <Option key={this.createKey(o)}>{o}</Option>
          })}
        </Select>
      </Col>
      <Col span={8}>
        <Select style={{ width: "100px", margin: "10px" }} onChange={() => this.handleGroupBySelectChange(i)} value={this.state.groupByValues[i]}>
          {_.get(this.allValues, op, []).map((o: string) => {
            return <Option key={`${op}-${o}`}>{o}</Option>
          })}
        </Select>
      </Col>
      <Col span={8}>
        <Button
          type="danger"
          onClick={() => console.log("plz change")}
          style={{ margin: "10px" }}
        >Delete</Button>
      </Col>
    </Row>)}));
  }


  render() {
    const { selectType } = this.props;
    if (selectType === "groupby") {
      return (<React.Fragment>
        {this.renderGroupBySelection()}
        {this.state.groupByOptions.length === 4 ? null : <Row>
          <Button
            type="dashed"
            onClick={() => {
              console.log("haha");
            }}
            style={{ width: '60%', marginLeft: "20%" }}
          >
            Add New Group By...
          </Button>
        </Row>}
      </React.Fragment>);
    } else if (selectType === "aggregation") {
      return (<Select value={this.state.aggregationOption} onChange={this.handleAggregationChange}>
        <Option value="sum">Sum</Option>
        <Option value="count">Count</Option>
        <Option value="average">Average</Option>
      </Select>);
    }
    return null;
  }
}
export default InputSelectComponent;