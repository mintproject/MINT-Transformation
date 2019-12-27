import React from "react";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { message, Upload, Icon, Row, Button, Tabs } from "antd";
import MyLayout from "./Layout";
import "antd/dist/antd.css";
import { UploadFile, UploadChangeParam } from "antd/lib/upload/interface";
import _ from "lodash";
import { RouterProps } from "react-router";
import { PipelineType } from "../store/PipelineStore";
import { AdapterType, flaskUrl } from "../store/AdapterStore";
import queryString from 'query-string'
const { TabPane } = Tabs;

interface CreatePipelineProps extends RouterProps {
  uploadedPipeline: PipelineType,
  setUploadedPipeline: (uploadedPipeline?: PipelineType | null, dcatId?: string) => any,
  location: Location,
}
interface CreatePipelineState {
  currentFileList: UploadFile[],
  // currentQueryParam: object,
}

@inject((stores: IStore) => ({
  uploadedPipeline: stores.pipelineStore.uploadedPipeline,
  setUploadedPipeline: stores.pipelineStore.setUploadedPipeline,
  location: stores.routing.location
}))
@observer
export class CreatePipelineComponent extends React.Component<
  CreatePipelineProps,
  CreatePipelineState
> {
  public state: CreatePipelineState = {
    currentFileList: [],
    // currentQueryParam: {},
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

  componentDidUpdate(prevProps: CreatePipelineProps, prevState: CreatePipelineState) {
    // if (prevProps.uploadedPipeline !== this.props.uploadedPipeline) {
    //   this.setState({
    //     visible: this.props.uploadedPipeline !== null
    //   })
    // }
  }

  handleFileChange = (info: UploadChangeParam<UploadFile>) => {
    const fileList = [...info.fileList];
    const file = fileList.slice(-1)[0];
    if (file.response && file.response.error) {
      message.info(`${file.response.error}`);
    } else if (file.response && file.response.data) {
      const { data } = file.response;
      // set the store var: uploadedPipeline
      this.props.setUploadedPipeline(data)
    }
    this.setState({ currentFileList: [file] });
  }

  handleSubmit = () => {
    this.props.history.push('/pipelines');
  }

  handleCancel = () => {
    this.props.setUploadedPipeline(null);
    this.setState({
      currentFileList: []
    });
    this.props.history.push('/pipeline/create');
  }
  
  cancleSubmitButtons = (
    <Row>
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
    </Row>
  )

  render() {
    const { uploadedPipeline } = this.props;
    var pipelineMeta = {};
    var pipelineAdapters: AdapterType[] = [];
    if (!_.isEmpty(uploadedPipeline)) {
      const pipelineKeys = Object.keys(uploadedPipeline).filter(key => key !== "adapters");
      pipelineMeta = pipelineKeys.reduce((obj, key) => {
        return {
          ...obj,
          [key]: _.get(uploadedPipeline, key)
        };
      }, {});
      pipelineAdapters = uploadedPipeline.adapters || [];
    }
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
              {this.cancleSubmitButtons}
              <Row>
                <pre>{JSON.stringify(pipelineMeta, null, 2)}</pre>
              </Row>
            </TabPane>
            <TabPane tab="Adapters" key="adapters">
              {this.cancleSubmitButtons}
              <Row>
                <pre>{JSON.stringify(pipelineAdapters, null, 2)}</pre>
              </Row>
            </TabPane>
          </Tabs>
        }
      </MyLayout>
    );
  }
}

export default CreatePipelineComponent;
