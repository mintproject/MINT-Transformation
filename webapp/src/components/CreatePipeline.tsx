import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { message, Upload, Icon, Row, Button, Tabs, Input, Col } from "antd";
import MyLayout from "./Layout";
import "antd/dist/antd.css";
import { UploadFile, UploadChangeParam } from "antd/lib/upload/interface";
import { RouterProps } from "react-router";
import { flaskUrl } from "../store/AdapterStore";
import queryString from 'query-string'
const { TabPane } = Tabs;

interface CreatePipelineProps extends RouterProps {
  uploadedPipeline: object | null,
  uploadedPipelineConfig: object | null,
  setUploadedPipeline: (uploadedPipeline?: object | null, dcatId?: string) => any,
  setUploadedPipelineConfig: (uploadedPipelineConfig: object | null) => any,
  location: Location,
  createPipeline: (pipelineName: string, pipelineDescription: string, pipelineConfig: object) => any,
}
interface CreatePipelineState {
  currentFileList: UploadFile[],
  pipelineName: string,
  pipelineDescription: string,
}

@inject((stores: IStore) => ({
  uploadedPipeline: stores.pipelineStore.uploadedPipeline,
  uploadedPipelineConfig: stores.pipelineStore.uploadedPipelineConfig,
  setUploadedPipeline: stores.pipelineStore.setUploadedPipeline,
  location: stores.routing.location,
  createPipeline: stores.pipelineStore.createPipeline,
  setUploadedPipelineConfig: stores.pipelineStore.setUploadedPipelineConfig,
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
        this.props.setUploadedPipeline(null, params.dcatId)
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
      // set the store var: uploadedPipeline
      this.props.setUploadedPipeline(data);
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
    this.props.setUploadedPipelineConfig(null);
    this.props.setUploadedPipelineConfig(null);
    this.props.history.push('/pipelines');
  }

  handleCancel = () => {
    this.props.setUploadedPipeline(null);
    this.setState({
      currentFileList: []
    });
    this.props.history.push('/pipeline/create');
  }

  render() {
    const { uploadedPipeline } = this.props;
    return (
      <MyLayout>
        {/* FIXME: upload url should not be hardcoded */}
        {
          this.props.uploadedPipeline === null ? 
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
          <Tabs defaultActiveKey="metadata" tabPosition="left" style={{ overflowY: "auto" }}>
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
                <pre>{JSON.stringify(uploadedPipeline, null, 2)}</pre>
              </Row>
            </TabPane>
          </Tabs>
        }
      </MyLayout>
    );
  }
}

export default CreatePipelineComponent;
