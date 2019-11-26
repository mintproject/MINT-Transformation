import React from "react";
import MyLayout from "./Layout";
import { observer, inject } from "mobx-react";
import { IStore, AppStore } from "../store";
import { Card, Col, Row } from 'antd';

const defaultProps = {};
interface BrowseProps extends Readonly<typeof defaultProps> {
  app: AppStore;
}
interface BrowseState {}

@inject((stores: IStore) => ({
  // FIXME: a is the testing variable
  app: stores.app
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
    const isCardLoading: boolean = this.props.app.adpaters.length == 0;
    return (
      <MyLayout>
          {this.props.app.adpaters.map(ad => 
            <Card
              title={ad.name}
              bordered={false}
              loading={isCardLoading}
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
