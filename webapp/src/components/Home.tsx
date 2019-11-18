import React from "react";
import { withStyles, WithStyles } from "@material-ui/styles";
import { observer, inject } from "mobx-react";
import { IStore, AppStore } from "../store";

const styles = {
  root: {
    background: "linear-gradient(45deg, #FE6B8B 30%, #FF8E53 90%)"
  }
};
const defaultProps = {};
interface Props extends Readonly<typeof defaultProps> {
  a: number,
  app: AppStore;
}
interface State {}

@inject((stores: IStore) => ({
  a: stores.app.a,
  app: stores.app
}))
@observer
export class HomeComponent extends React.Component<
  Props & WithStyles<typeof styles>,
  State
> {
  public static defaultProps = defaultProps;
  public state: State = {};

  render() {
    return (
      <p>
        <h1 className={this.props.classes.root}>HOME</h1>
        <h1 className={this.props.classes.root}>{this.props.a}</h1>
        <button onClick={this.props.app.setA}>Click Me</button>
      </p>
    );
  }
}

export default withStyles(styles)(HomeComponent);
