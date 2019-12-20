import React from "react";
import { inject, observer } from "mobx-react";
import {
  Route,
  BrowserRouter,
  Switch
} from "react-router-dom";
import { RouterStore } from "mobx-react-router";
import { IStore, AppStore } from "./store";
import NotFound404 from "./components/NotFound404";
import Home from "./components/Home";
import Adapters from "./components/Adapters";
import LoadingComponent from "./components/Loading";
import PipelineTimeline from "./components/PipelineTimeline";
import PipelineDetail from "./components/PipelineDetail";
import CreatePipeline from "./components/CreatePipeline";

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
      <BrowserRouter>
        <Switch>
          <Route exact path="/" component={Home} />
          <Route exact path="/adapters" component={Adapters} />
          <Route exact path="/pipelines" component={PipelineTimeline} />
          <Route path='/pipelines/:pipelineId' component={PipelineDetail}/>
          <Route path="/pipeline/create" component={CreatePipeline} />
          <Route component={NotFound404} />
        </Switch>
      </BrowserRouter>
    );
  }
}
