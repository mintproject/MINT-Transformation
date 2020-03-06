import React from "react";
import {withStyles, WithStyles} from "@material-ui/styles";
import {observer} from "mobx-react";
import {Spin} from "antd";

const styles = {
  root: {
    marginTop: 50,
    textAlign: "center" as "center"
  }
};
const defaultProps = {};
interface Props extends Readonly<typeof defaultProps> {}
interface State {}

@observer
class LoadingComponent extends React.Component<
  Props & WithStyles<typeof styles>,
  State
> {
  public static defaultProps = defaultProps;
  public state: State = {};

  render() {
    return (
      <div className={this.props.classes.root}>
        <Spin tip="Loading..." size="large" />
      </div>
    );
  }
}

export default withStyles(styles)(LoadingComponent);
