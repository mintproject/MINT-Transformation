import React from "react";
import { withStyles, WithStyles } from "@material-ui/styles";
import { observer } from "mobx-react";
import { Icon } from "antd";
import ClickableIcon from "./ClickableIcon";

const styles = {
  root: {}
};
const defaultProps = {
  style: {}
};
interface Props extends Readonly<typeof defaultProps> {
  isSaved: boolean;
  saveFn: () => Promise<any>;
}
interface State {
  status: "error" | "saving" | "normal";
}
const COLORS = {
  red: "#ff4d4f",
  darkRed: "#cf1322",
  green: "#52c41a",
  darkGreen: "#237804"
};

@observer
export class SaveBtn extends React.Component<
  Props & WithStyles<typeof styles>,
  State
> {
  public static defaultProps = defaultProps;
  public state: State = {
    status: "normal"
  };
  // isMounted is a reserved name in React
  private isComponentMounted: boolean = false;

  componentDidMount = () => {
    this.isComponentMounted = true;
  };

  componentWillUnmount = () => {
    this.isComponentMounted = false;
  };

  onClick = () => {
    this.setState({ status: "saving" });
    this.props
      .saveFn()
      .then(() => {
        if (this.isComponentMounted) {
          this.setState({ status: "normal" });
        }
      })
      .catch(() => {
        if (this.isComponentMounted) {
          this.setState({ status: "error" });
        }
      });
  };

  render() {
    if (this.state.status === "saving") {
      return <Icon type="loading" style={this.props.style} />;
    }

    if (this.state.status === "error") {
      return (
        <Icon
          onClick={this.onClick}
          type="warning"
          style={{ ...this.props.style, color: COLORS.darkRed }}
        />
      );
    }

    if (this.props.isSaved) {
      return (
        <ClickableIcon
          color={COLORS.green}
          pressColor={COLORS.darkGreen}
          onClick={this.onClick}
          icon="save"
          style={this.props.style}
        />
      );
    } else {
      return (
        <ClickableIcon
          color={COLORS.red}
          pressColor={COLORS.darkRed}
          onClick={this.onClick}
          icon="save"
          style={this.props.style}
        />
      );
    }
  }
}

export default withStyles(styles)(SaveBtn);
