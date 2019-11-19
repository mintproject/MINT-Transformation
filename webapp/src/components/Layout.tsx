import * as React from "react";
import { Menu, Header, Search, Segment } from "semantic-ui-react";
import { Link } from "react-router-dom";

type LayoutProps = {
  children?: any,
};

export default class Layout extends React.Component<LayoutProps> {

  render() {
    const { children } = this.props;

    return (
      <React.Fragment>
        <Menu text fixed="top" style={{ fontSize: "larger" }}>
          <Menu.Item style={{ paddingTop: "0.5rem", paddingBottom: "0.5rem" }} as={Link} to="/">
            <Header content="MINT-DT"/>
          </Menu.Item>
          <Menu.Item style={{ paddingTop: "0.5rem", paddingBottom: "0.5rem" }} as={Link} to="/">
            <Header content="Home"/>
          </Menu.Item>
          <Menu.Item style={{ paddingTop: "0.5rem", paddingBottom: "0.5rem" }} as={Link} to="/haha">
            <Header content="Browse"/>
          </Menu.Item>
          <Menu.Item style={{ paddingTop: "0.5rem", paddingBottom: "0.5rem" }} as={Link} to="/gaga">
            <Header content="Pipeline"/>
          </Menu.Item>
          <Menu.Item position="right">
            <Search/>
          </Menu.Item>
        </Menu>
        <div className="spacer" style={{ height: "5rem" }} />
        <div>
          {children}
        </div>
      </React.Fragment>
    );
  }
}