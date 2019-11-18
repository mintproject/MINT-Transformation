import React from "react";
// FIXME: no such resources
// import "./App.css";
// import "./fonts.css";
import { inject, observer } from "mobx-react";
import { Route, Switch } from "react-router";
import { RouterStore } from "mobx-react-router";
import { RouteConf } from "./route";
import { IStore, AppStore } from "./store";
import NotFound404 from "./components/NotFound404";
import Home from "./components/Home";
import LoadingComponent from "./components/Loading";

interface Props {
  routing?: RouterStore;
  app?: AppStore;
}

@inject((stores: IStore) => ({
  routing: stores.routing,
  app: stores.app
}))
@observer
export default class App extends React.Component<Props> {
  componentDidMount() {
    this.props.app!.init();
  }

  render() {
    if (!this.props.app!.isInited) {
      return <LoadingComponent />;
    }

    return (
      <Switch>
        <Route {...RouteConf.home.routeDef} component={Home} />
        <Route component={NotFound404} />
      </Switch>
    );
  }
}
