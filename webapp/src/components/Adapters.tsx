import React from "react";
import MyLayout from "./Layout";
import {inject, observer} from "mobx-react";
import {IStore} from "../store";
import {Card} from 'antd';
import {AdapterType} from "../store/AdapterStore";
import _ from "lodash";

const defaultProps = {};
interface AdapterProps extends Readonly<typeof defaultProps> {
  adapters: AdapterType[],
  getAdapters: () => any,
}
interface AdapterState {
}

@inject((stores: IStore) => ({
  adapters: stores.adapterStore.adapters,
  getAdapters: stores.adapterStore.getAdapters,
}))
@observer
export class AdapterComponent extends React.Component<
  AdapterProps,
  AdapterState
> {
  public static defaultProps = defaultProps;
  public state: AdapterState = {
  };

  componentDidMount() {
    this.props.getAdapters();
  }


  render() {
    // this component renders all existing adapters
    // TODO: similar UI between adapters and pipeline?
    // FIXME: not exactly sure how to manage state and onChange and class props
    const isCardLoading: boolean = this.props.adapters.length === 0;
    return (
      <MyLayout>
        {this.props.adapters.map((ad, index) => 
            <Card
              title={ad.id}
              bordered={true}
              loading={isCardLoading}
              style={{ margin: "10px 10px" }}
              key={`card-${index}`}
              hoverable
            >
              <pre>
                • <b><u>Function Name</u></b>: {ad.id}<br/>
                • <b><u>Friendly Name</u></b>: {ad.friendly_name}<br/>
                • <b><u>Function Type</u></b>: {ad.func_type}<br/>
                • <b><u>Description</u></b>: {ad.description}<br/>
                • <b><u>Inputs</u></b>: {_.isEmpty(ad.inputs) ? <p>None</p> :Object.keys(ad.inputs).map(
                  (inputKey, idx) => (<pre key={`input-${idx}`}>
                    <b><u>{inputKey}</u></b>:<br/>
                      Type: <input value={ad.inputs[inputKey].id} readOnly/>;<br/>
                      Optional: <input value={JSON.stringify(ad.inputs[inputKey].optional)} readOnly/>
                  </pre>))}<br/>
                • <b><u>Outputs</u></b>: {_.isEmpty(ad.outputs) ? <p>None</p> :Object.keys(ad.outputs).map((outputKey, idx) => (
                  <pre key={`input-${idx}`}>
                    <b><u>{outputKey}</u></b>:<br/>
                      Type: <input value={ad.outputs[outputKey].id} readOnly/>;<br/>
                      Optional: <input value={JSON.stringify(ad.outputs[outputKey].optional)} readOnly/>
                  </pre>))}<br/>
              </pre>
            </Card>
        )}
      </MyLayout>
  ); }
}

export default AdapterComponent;
