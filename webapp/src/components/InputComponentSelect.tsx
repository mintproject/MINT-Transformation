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
  graphNodes: INode[],
  setGraphNodes: (nodes: INode[]) => any,
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
  graphNodes: stores.pipelineStore.graphNodes,
  setGraphNodes: stores.pipelineStore.setGraphNodes,
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
    // FIXME: there could be an issue with syncing mobx state and local state.
    const { input, selectedNode, selectType } = this.props;
    if (selectedNode === null) { return; }
    const selectedAdapter = selectedNode!.adapter;
    console.log("INSIDE CDM: ")
    console.log(selectedAdapter.inputs[input].val)
    if (selectType === "groupby") {
      const inputList = selectedAdapter.inputs[input].val || [];
      // @ts-ignore
      const optionList = inputList.map(i => this.allKeys[i!.prop]);
      // @ts-ignore
      const valueList = inputList.map(i => i!.value);
      // console.log("resetting adapter input...");
      console.log(optionList);
      console.log(valueList);
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


  handleGroupBySelectChange = (i: number, value: string) => {
    const { graphNodes, selectedNode, input, setGraphNodes } = this.props;
    const { groupByOptions, groupByValues } = this.state;
    if (selectedNode === null) { return; }
    let newOptions = groupByOptions;
    newOptions.splice(i, 1, value);
    let newValues = groupByValues;
    newValues.splice(i, 1, "exact");
    // reset graphNodes and state
    let newNodes = graphNodes;
    let newNode = newNodes.filter(n => n.id === selectedNode.id)[0];
    newNode.adapter.inputs[input].val = groupByValues.map((val, idx) => ({
      prop: this.createKey(newOptions[idx]),
      value: val
    }));
    setGraphNodes(newNodes);
    this.setState({
      groupByOptions: newOptions,
      groupByValues: newValues
    })
  }

  handleGroupByValueChange = (i: number, value: string) => {
    const { graphNodes, selectedNode, input, setGraphNodes } = this.props;
    const { groupByOptions, groupByValues } = this.state;
    if (selectedNode === null) { return; }
    let newValues = groupByValues;
    newValues.splice(i, 1, value);
    // reset graphNodes and state
    let newNodes = graphNodes;
    let newNode = newNodes.filter(n => n.id === selectedNode.id)[0];
    newNode.adapter.inputs[input].val = groupByOptions.map((op, idx) => ({
      prop: this.createKey(op),
      value: newValues[idx]
    }));
    setGraphNodes(newNodes);
    this.setState({
      groupByValues: newValues
    })
  }

  handleAggregationChange = (value: string) => {
    const { graphNodes, selectedNode, input, setGraphNodes } = this.props;
    if (selectedNode === null) { return; }
    let newNodes = graphNodes;
    let newNode = newNodes.filter(n => n.id === selectedNode.id)[0];
    newNode.adapter.inputs[input].val = value;
    setGraphNodes(newNodes);
    this.setState({
      aggregationOption: value
    })
  }

  renderGroupBySelection = () => {
    const { groupByOptions } = this.state;
    console.log("AM i here?")
    const availableOption = this.allOptions.filter(op => !this.state.groupByOptions.includes(op));
    console.log(this.state.groupByOptions);
    console.log(availableOption)
    return (groupByOptions.map((op, i) => { return (<Row key={`row-${op}-${i}`}>
      <Col span={8}>
        <Select
          style={{ width: "100px", margin: "10px" }}
          onChange={(value: string) => this.handleGroupBySelectChange(i, value)}
          value={op}
        >
          {this.allOptions.map(o => {
            return <Option key={o}>{o}</Option>
          })}
        </Select>
      </Col>
      <Col span={8}>
        <Select
          style={{ width: "100px", margin: "10px" }}
          onChange={(value: string) => this.handleGroupByValueChange(i, value)}
          value={this.state.groupByValues[i]}
        >
          {_.get(this.allValues, op, []).map((o: string) => {
            return <Option key={`${o}`}>{o}</Option>
          })}
        </Select>
      </Col>
      <Col span={8}>
        <Button
          type="danger"
          onClick={() => {
            var newOptions = this.state.groupByOptions;
            var newValues = this.state.groupByValues;
            newOptions.splice(i, 1);
            newValues.splice(i, 1);
            this.setState({
              groupByOptions: newOptions,
              groupByValues: newValues
            })
          }}
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
              // append a new pair to groupByOptions/values
              const availableOption = this.allOptions.filter(op => !this.state.groupByOptions.includes(op));
              var newOptions = this.state.groupByOptions;
              var newValues = this.state.groupByValues;
              newOptions.push(availableOption[0]);
              newValues.push("exact");
              this.setState({
                groupByOptions: newOptions,
                groupByValues: newValues
              })
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