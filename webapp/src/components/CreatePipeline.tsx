import React from "react";
import { observer, inject } from "mobx-react";
import { IStore, AppStore } from "../store";
import { message, Upload, Icon, Modal, Button, Tabs } from "antd";
import MyLayout from "./Layout";
import "antd/dist/antd.css";
import { UploadFile, UploadChangeParam } from "antd/lib/upload/interface";
import _ from "lodash";
import { RouterProps } from "react-router";
import { PipelineType, AdapterType } from "../store/AppStore";
import queryString from 'query-string'
const { TabPane } = Tabs;

interface CreatePipelineProps extends RouterProps {
  uploadedPipeline: PipelineType,
  setUploadedPipeline: (uploadedPipeline?: PipelineType | null, dcatId?: string) => any,
}
interface CreatePipelineState {
  currentFileList: UploadFile[],
  visible: boolean,
  // currentQueryParam: object,
}

@inject((stores: IStore) => ({
  uploadedPipeline: stores.app.uploadedPipeline,
  setUploadedPipeline: stores.app.setUploadedPipeline
}))
@observer
export class CreatePipelineComponent extends React.Component<
CreatePipelineProps,
CreatePipelineState
> {
  public state: CreatePipelineState = {
    currentFileList: [],
    visible: false,
    // currentQueryParam: {},
  };

  componentDidMount() {
    // @ts-ignore
    if (this.props.location.search) {
    // @ts-ignore
      const params = queryString.parse(this.props.location.search)
      if (params && params.dcatId) {
        if (Array.isArray(params.dcatId)) {
          this.props.setUploadedPipeline(null, params.dcatId[0])
        } else {
          this.props.setUploadedPipeline(null, params.dcatId)
        }
      } else {
        // redirect to create page to upload
        this.props.history.push('/pipeline/create');
      }
    }
  }

  componentDidUpdate(prevProps: CreatePipelineProps, prevState: CreatePipelineState) {
    if (prevProps.uploadedPipeline !== this.props.uploadedPipeline) {
      this.setState({
        visible: this.props.uploadedPipeline !== null
      })
    }
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
        <Upload.Dragger
          name="files"
          action="http://localhost:5000/pipeline/upload_config"
          accept=".json,.yml"
          onChange={this.handleFileChange}
          multiple={false}
          fileList={this.state.currentFileList}
        >
          <p className="ant-upload-drag-icon">
            <Icon type="inbox" />
          </p>
          <p className="ant-upload-text">Click or drag file to this area to upload</p>
          <p className="ant-upload-hint">Support for single upload.</p>
        </Upload.Dragger>
        {
          !this.state.visible ? null :
          <Modal
            title="Config File Summary"
            visible={this.state.visible}
            onCancel={this.handleCancel}
            footer={[
              <Button key="discard" onClick={this.handleCancel}>
                Discard
              </Button>,
              <Button key="submit" type="primary" onClick={this.handleSubmit}>
                Submit
              </Button>,
            ]}
            centered
          >
            {/* <div><pre>{JSON.stringify(uploadedPipeline, null, 2)}</pre></div> */}
            <Tabs defaultActiveKey="metadata">
              <TabPane tab="Metadata" key="metadata">
                <pre>{JSON.stringify(pipelineMeta, null, 2)}</pre>
              </TabPane>
              <TabPane tab="Adapters" key="adapters">
                <pre>{JSON.stringify(pipelineAdapters, null, 2)}</pre>
              </TabPane>
            </Tabs>
          </Modal>
        }
      </MyLayout>
    );
  }
}

export default CreatePipelineComponent;
