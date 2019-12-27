import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { Link } from "react-router-dom";
import MyLayout from "./Layout";
import { Row, Col, Input, Menu, Button, Popover, Dropdown, Typography } from 'antd';
import { PipelineType } from "../store/PipelineStore";
import _ from "lodash";
const { Title } = Typography;

const { Search } = Input;

interface PipelineTimelineProps {
  pipelines: PipelineType[],
  getPipelines: () => any,
}
interface PipelineTimelineState {}

@inject((stores: IStore) => ({
  pipelines: stores.pipelineStore.pipelines,
  getPipelines: stores.pipelineStore.getPipelines
}))
@observer
export class PipelineTimelineComponent extends React.Component<
  PipelineTimelineProps,
  PipelineTimelineState
> {
  public state: PipelineTimelineState = {};

  componentDidMount() {
    this.props.getPipelines();
  }

  readonly timestampDropdownMenu = (
    <Menu>
      <Menu.Item>
        Newest
      </Menu.Item>
      <Menu.Item>
        Oldest
      </Menu.Item>
    </Menu>
  );

  readonly statusColorCode = {
    running: "orange",
    finished: "green",
    failed: "red",
  }

  render() {
    return (
      <MyLayout>
        <Row type="flex" align="middle" style={{ margin: "20px 0"}}>
          <Col span={12}>
            <Search
              placeholder="input search text"
              onSearch={value => console.log(value)}
              style={{ textAlign: "left" }}
            />
          </Col>
          <Col span={8}/>
          <Col span={2}>
            <Dropdown
              overlay={this.timestampDropdownMenu}
            >
              <Button
                type="link" icon="down"
                style={{ float: "right", color: "black" }}
              >Sort</Button>
            </Dropdown>
          </Col>
          <Col span={2}>
            <Popover content="Create a new pipeline!">
              <Link to={"/pipeline/create"}>
                <Button
                  type="primary"
                  shape="circle"
                  icon="plus"
                  // onClick={() => console.log("DOES NOTHING NOW!")}
                  style={{ float: "right", margin: "0 25px" }}
                  size="large"
                />
              </Link>
            </Popover>
          </Col>
        </Row>
        {this.props.pipelines.map((pl, index) => <Row
          key={`row-${index}`}
          type="flex" align="middle"
        >
          <Col span={2} style={{ textAlign: "right" }}>
            <Title
              style={{
                color: `${_.get(this.statusColorCode, pl.status, "black")}`,
              }}
              level={4}
            >{pl.status}</Title>
          </Col>
          <Col span={1}/>
          <Col span={19}>
            <Row style={{ margin: "0 0"}}>
              <Title level={4}>
                <Link to={`/pipelines/${pl.id}`} style={{ color: "black" }}>
                  {pl.name}
                </Link>
              </Title>
            </Row>
            <Row style={{ margin: "0 0"}}>
              <p>{
                pl.end_timestamp === ""
                ? `Started at ${pl.start_timestamp}, still running`
                : `Started at ${pl.start_timestamp}, ended at ${pl.end_timestamp}`
              }</p>
            </Row>
          </Col>
          <Col span={2}>
            <Button
              type="danger"
              onClick={() => console.log("Doesn't do anything!")}
              disabled={pl.status !== "running"}
              style={{ marginRight: "10px"}}
            >STOP</Button>
          </Col>
        </Row>)}
      </MyLayout>
    );
  }
}

export default PipelineTimelineComponent;