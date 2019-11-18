import React from "react";
import { withStyles, WithStyles } from "@material-ui/styles";
import { observer, inject } from "mobx-react";
import { IStore } from "../../store";

const styles = {
  root: {
    background: "linear-gradient(45deg, #FE6B8B 30%, #FF8E53 90%)"
  }
};
const defaultProps = {};
interface Props extends Readonly<typeof defaultProps> {}
interface State {}

@inject((stores: IStore) => ({}))
@observer
export class MyComponent extends React.Component<
  Props & WithStyles<typeof styles>,
  State
> {
  public static defaultProps = defaultProps;
  public state: State = {};

  render() {
    return <h1 className={this.props.classes.root}>MyComponent</h1>;
  }
}

export default withStyles(styles)(MyComponent);
