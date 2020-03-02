import React from "react";
import { inject, observer } from "mobx-react";
import {
  Route,
  BrowserRouter,
  Switch
} from "react-router-dom";
import { IStore } from "./store";
import NotFound404 from "./components/NotFound404";
import Home from "./components/Home";
import Adapters from "./components/Adapters";
// import LoadingComponent from "./components/Loading";
import PipelineTimeline from "./components/PipelineTimeline";
import PipelineDetail from "./components/PipelineDetail";
import PipelineEditor from "./components/PipelineEditor";
import "./App.css";

interface Props {}

@inject((stores: IStore) => ({
  routing: stores.routing,
  pipelineStore: stores.pipelineStore,
  adapterStore: stores.adapterStore,
}))
@observer
export default class App extends React.Component<Props> {
  render() {
    return (
      <BrowserRouter>
        <Switch>
          <Route exact path="/" component={Home} />
          <Route exact path="/adapters" component={Adapters} />
          <Route exact path="/pipelines" component={PipelineTimeline} />
          <Route path='/pipelines/:pipelineId' component={PipelineDetail}/>
          <Route exact path="/pipeline/create" component={PipelineEditor} />
          <Route component={NotFound404} />
        </Switch>
      </BrowserRouter>
    );
  }
}
