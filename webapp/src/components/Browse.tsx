import React from "react";
import MyLayout from "./Layout";
import { observer, inject } from "mobx-react";
import { IStore, AppStore } from "../store";
import { Card, Col, Row } from 'antd';
import { AdapterType } from "../store/AppStore";

const defaultProps = {};
interface BrowseProps extends Readonly<typeof defaultProps> {
  adapters: AdapterType[],
  getAdapters: () => any,
}
interface BrowseState {}

@inject((stores: IStore) => ({
  adapters: stores.app.adapters,
  getAdapters: stores.app.getAdapters,
}))
@observer
export class BrowseComponent extends React.Component<
  BrowseProps,
  BrowseState
> {
  public static defaultProps = defaultProps;
  public state: BrowseState = {};

  render() {
    // this component renders all existing adapters
    // FIXME: not exactly sure how to manage state and onChange and class props
    const isCardLoading: boolean = this.props.adapters.length == 0;
    return (
      <MyLayout>
          {this.props.adapters.map(ad => 
            <Card
              title={ad.name}
              bordered={true}
              loading={isCardLoading}
              style={{ margin: "5px 5px" }}
            >
              <p>
                • <b><u>Function Name</u></b>: {ad.func_name}<br/>
                • <b><u>Description</u></b>: {ad.description}<br/>
                • <b><u>Inputs</u></b>: {JSON.stringify(ad.input)}<br/>
                • <b><u>Outputs</u></b>: {JSON.stringify(ad.ouput)}<br/>
              </p>
            </Card>
          )}
      </MyLayout>
  ); }
}

export default BrowseComponent;