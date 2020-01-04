import React from "react";
import MyLayout from "./Layout";
import { observer, inject } from "mobx-react";
import { IStore } from "../store";
import { Card } from 'antd';
import { AdapterType } from "../store/AdapterStore";

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
    // FIXME: not exactly sure how to manage state and onChange and class props
    const isCardLoading: boolean = this.props.adapters.length === 0;
    return (
      <MyLayout>
          {this.props.adapters.map((ad, index) => 
              <Card
                title={ad.name}
                bordered={true}
                loading={isCardLoading}
                style={{ margin: "10px 10px" }}
                key={`card-${index}`}
                hoverable
              >
                <pre>
                  • <b><u>Function Name</u></b>: {ad.func_name}<br/>
                  • <b><u>Description</u></b>: {ad.description}<br/>
                  • <b><u>Inputs</u></b>: {JSON.stringify(ad.input, null, 2)}<br/>
                  • <b><u>Outputs</u></b>: {JSON.stringify(ad.ouput, null, 2)}<br/>
                </pre>
              </Card>
          )}
      </MyLayout>
  ); }
}

export default AdapterComponent;
