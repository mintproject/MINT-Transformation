import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { message, Upload, Icon, Row, Button, Tabs, Input, Col } from "antd";
import MyLayout from "./Layout";
import { UploadedPipelineDataType } from "../store/PipelineStore"
import "antd/dist/antd.css";
import { UploadFile, UploadChangeParam } from "antd/lib/upload/interface";
import { RouterProps } from "react-router";
import { flaskUrl } from "../store/AdapterStore";
import queryString from 'query-string';
import {
  GraphView,
} from "react-digraph";

const { TabPane } = Tabs;

const GraphConfig =  {
  NodeTypes: {
    empty: { // required to show empty nodes
      typeText: "None",
      shapeId: "#empty", // relates to the type property of a node
      shape: (
        <symbol viewBox="0 0 100 100" id="empty" key="0">
          <circle cx="50" cy="50" r="45"></circle>
        </symbol>
      )
    },
    custom: { // required to show empty nodes
      typeText: "Custom",
      shapeId: "#custom", // relates to the type property of a node
      shape: (
        <symbol viewBox="0 0 50 25" id="custom" key="0">
          <ellipse cx="50" cy="25" rx="50" ry="25"></ellipse>
        </symbol>
      )
    }
  },
  NodeSubtypes: {},
  EdgeTypes: {
    emptyEdge: {  // required to show empty edges
      shapeId: "#emptyEdge",
      shape: (
        <symbol viewBox="0 0 50 50" id="emptyEdge" key="0">
          <circle cx="25" cy="25" r="8" fill="currentColor"> </circle>
        </symbol>
      )
    }
  }
}

interface CreatePipelineProps extends RouterProps {
  uploadedPipelineData: UploadedPipelineDataType | null,
  uploadedPipelineConfig: object | null,
  setUploadedPipelineData: (uploadedPipeline: UploadedPipelineDataType | null) => any,
  setUploadedPipelineConfig: (uploadedPipelineConfig: object | null) => any,
  setUploadedPipelineFromDcat: (dcatId: string) => any,
  location: Location,
  createPipeline: (pipelineName: string, pipelineDescription: string, pipelineConfig: object) => any,
}
interface CreatePipelineState {
  currentFileList: UploadFile[],
  pipelineName: string,
  pipelineDescription: string,
}

@inject((stores: IStore) => ({
  uploadedPipelineData: stores.pipelineStore.uploadedPipelineData,
  uploadedPipelineConfig: stores.pipelineStore.uploadedPipelineConfig,
  location: stores.routing.location,
  createPipeline: stores.pipelineStore.createPipeline,
  setUploadedPipelineConfig: stores.pipelineStore.setUploadedPipelineConfig,
  setUploadedPipelineData: stores.pipelineStore.setUploadedPipelineData,
  setUploadedPipelineFromDcat: stores.pipelineStore.setUploadedPipelineFromDcat,
}))
@observer
export class CreatePipelineComponent extends React.Component<
  CreatePipelineProps,
  CreatePipelineState
> {
  public state: CreatePipelineState = {
    currentFileList: [],
    pipelineName: "",
    pipelineDescription: "",
  };

  componentDidMount() {
    if (this.props.location.search) {
      const params = queryString.parse(this.props.location.search)
      if (params && params.dcatId && typeof params.dcatId === "string") {
        this.props.setUploadedPipelineFromDcat(params.dcatId)
      }
    } else {
      // redirect to create page to upload
      this.props.history.push('/pipeline/create');
    }
  }

  handleFileChange = (info: UploadChangeParam<UploadFile>) => {
    const fileList = [...info.fileList];
    const file = fileList.slice(-1)[0];
    if (file.response && file.response.error) {
      message.info(`${file.response.error}`);
    } else if (file.response && file.response.data) {
      const { data, config } = file.response;
      this.props.setUploadedPipelineData(data);
      this.props.setUploadedPipelineConfig(config);
    }
    this.setState({ currentFileList: [file] });
  }

  handleSubmit = () => {
    const { pipelineName, pipelineDescription } = this.state;
    const { uploadedPipelineConfig } = this.props;
    if (uploadedPipelineConfig === null) {
      return;
    }
    this.props.createPipeline(pipelineName, pipelineDescription, uploadedPipelineConfig);
    this.props.setUploadedPipelineData(null);
    this.props.setUploadedPipelineConfig(null);
    this.props.history.push('/pipelines');
  }

  handleCancel = () => {
    this.props.setUploadedPipelineData(null);
    this.props.setUploadedPipelineConfig(null);
    this.setState({
      currentFileList: []
    });
    this.props.history.push('/pipeline/create');
  }

  render() {
    const { uploadedPipelineData, uploadedPipelineConfig } = this.props;
    console.log("inside create");
    console.log(uploadedPipelineData === null ? null : uploadedPipelineData.nodes)
    return (
      <MyLayout>
        {/* FIXME: upload url should not be hardcoded */}
        {
          uploadedPipelineData === null ? 
          <Upload.Dragger
            name="files"
            action={`${flaskUrl}/pipeline/upload_config`}
            accept=".json,.yml"
            onChange={this.handleFileChange}
            multiple={false}
            fileList={this.state.currentFileList}
            style={{ height: "100%" }}
          >
            <p className="ant-upload-drag-icon">
              <Icon type="inbox" />
            </p>
            <p className="ant-upload-text">Click or drag file to this area to upload</p>
            <p className="ant-upload-hint">Support for single upload.</p>
          </Upload.Dragger> : 
          <Tabs defaultActiveKey="adapters" tabPosition="left" style={{ overflowY: "auto", height: "100%" }}>
            <TabPane tab="Adapters" key="adapters">
              <GraphView
                ref='GraphView'
                nodeKey="id"
                nodes={uploadedPipelineData.nodes}
                edges={uploadedPipelineData.edges}
                selected={null}
                nodeTypes={GraphConfig.NodeTypes}
                nodeSubtypes={GraphConfig.NodeSubtypes}
                edgeTypes={GraphConfig.EdgeTypes}
                // onSelectNode={this.onSelectNode}
                // onCreateNode={this.onCreateNode}
                // onUpdateNode={this.onUpdateNode}
                // onDeleteNode={this.onDeleteNode}
                // onSelectEdge={this.onSelectEdge}
                // onCreateEdge={this.onCreateEdge}
                // onSwapEdge={this.onSwapEdge}
                // onDeleteEdge={this.onDelet() => console.log("does nothing")
                onSelectNode={() => console.log("does nothing")}
                onCreateNode={() => console.log("does nothing")}
                onUpdateNode={() => console.log("does nothing")}
                onDeleteNode={() => console.log("does nothing")}
                onSelectEdge={() => console.log("does nothing")}
                onCreateEdge={() => console.log("does nothing")}
                onSwapEdge={() => console.log("does nothing")}
                onDeleteEdge={() => console.log("does nothing")}
              />
              <pre>{JSON.stringify(uploadedPipelineData, null, 2)}</pre>
            </TabPane>
            <TabPane tab="Metadata" key="metadata">
              <Row style={{ margin: "20px 0px"}}>
                <Col span={16}>
                  <Input
                    value={this.state.pipelineName}
                    onChange={(event: React.ChangeEvent<HTMLInputElement>) => this.setState({ pipelineName: event.target.value})}
                    placeholder="Enter Pipeline Name"
                    style={{ margin: "10px" }}
                  />
                </Col>
                <Col span={8}>
                  <Button
                    key="discard" onClick={this.handleCancel}
                    style={{ margin: "10px", float: "right" }}
                  >
                    Discard
                  </Button>
                  <Button
                    key="submit" type="primary"
                    onClick={this.handleSubmit}
                    style={{ margin: "10px", float: "right" }}
                  >
                    Submit
                  </Button>
                </Col>
              </Row>
              <Row style={{ margin: "20px 10px"}}>
               <Input
                  value={this.state.pipelineDescription}
                  onChange={(event: React.ChangeEvent<HTMLInputElement>) => this.setState({ pipelineDescription: event.target.value})}
                  placeholder="Enter Pipeline Description"
                />
              </Row>
              <Row style={{ margin: "20px 10px"}}>
                <pre>{JSON.stringify(uploadedPipelineConfig, null, 2)}</pre>
              </Row>
            </TabPane>
          </Tabs>
        }
      </MyLayout>
    );
  }
}

export default CreatePipelineComponent;
