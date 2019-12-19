import React from "react";
import { observer, inject } from "mobx-react";
import { IStore, AppStore } from "../store";
import { message, Upload, Icon, Modal, Button } from "antd";
import MyLayout from "./Layout";
import { FormComponentProps } from 'antd/es/form';
import "antd/dist/antd.css";
import { UploadFile, UploadChangeParam } from "antd/lib/upload/interface";
import _ from "lodash";

interface CreatePipelineProps extends FormComponentProps {
}
interface CreatePipelineState {
  currentFileList: UploadFile[],
  currentQueryParam: object,
}

@inject((stores: IStore) => ({
}))
@observer
export class CreatePipelineComponent extends React.Component<
CreatePipelineProps,
CreatePipelineState
> {
  public state: CreatePipelineState = {
    currentFileList: [],
    currentQueryParam: {},
  };

  componentDidMount() {
    // @ts-ignore
    // const queryParams = queryString.parse(this.props.location.search)
    if (!_.isEmpty(this.props.location.search)) {
      try {
        // @ts-ignore
        const parsedUri = decodeURIComponent(this.props.location.search.substring(1));
        const queryParams = JSON.parse(parsedUri);
        if (!_.isEmpty(queryParams)) {
          // bring up the modal
          this.setState({
            currentQueryParam: queryParams
          })
        }
      } catch (ex) {
        console.log("ERROR DECODING URI: " + ex.message);
        // @ts-ignore
        this.props.history.push(`/pipeline/create`);
        this.setState({ currentQueryParam: {} });
      }
    }
  }

  componentDidUpdate(prevProps: CreatePipelineProps, prevState: CreatePipelineState) {
    if (
      this.state.currentFileList !== prevState.currentFileList
      && !_.isEmpty(this.state.currentFileList)
      && this.state.currentFileList[0].response
    ) {
      const file = this.state.currentFileList[0]
      if (file.response && file.response.error) {
        message.info(`${file.response.error}`);
      } else if (file.response && file.response.status) {
        // FIXME: this only works when data is a first-level object
        const { data } = file.response;
        console.log("INSIDE UPDATE!")
        // @ts-ignore
        this.props.history.push(`/pipeline/create?${encodeURIComponent(JSON.stringify(data))}`);
        this.setState({
          currentQueryParam: data
        })
      }
    } else if (
      this.state.currentFileList !== prevState.currentFileList
      && _.isEmpty(this.state.currentFileList)
    ) {
      // @ts-ignore
      this.props.history.push(`/pipeline/create`);
      this.setState({
        currentQueryParam: {}
      })
    }
  }

  handleFileChange = (info: UploadChangeParam<UploadFile>) => {
    let fileList = [...info.fileList];
    fileList = fileList.slice(-1);
    this.setState({ currentFileList: fileList });
  }

  handleSubmit = () => {
    // FIXME: TYPE ISSUE
    // @ts-ignore
    this.props.history.push('/pipelines');
  }

  handleCancel = () => {
    // this.setState({ currentQueryParam: {} });
    this.setState({ currentFileList: [] });

    // @ts-ignore
    // this.props.history.push(`/pipeline/create`);
  }

  render() {
    const { currentQueryParam } = this.state;
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
          _.isEmpty(currentQueryParam) ? null : <Modal
            title="Config File Summary"
            visible={!_.isEmpty(this.state.currentQueryParam)}
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
            <div><pre>{JSON.stringify(currentQueryParam, null, 2)}</pre></div>
          </Modal>
        }
      </MyLayout>
    );
  }
}

export default CreatePipelineComponent;
