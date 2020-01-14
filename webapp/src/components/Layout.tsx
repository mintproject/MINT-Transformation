import * as React from "react";
import { Link } from "react-router-dom";
import {  Menu, Layout, Input } from "antd";
const { Header, Content, Footer } = Layout;
const { Search } = Input;

type LayoutProps = {
  children?: any,
};

type LayoutState = {
  selectedKey: string
}

export default class MyLayout extends React.Component<LayoutProps> {
  state: Readonly<LayoutState> = {
    selectedKey: ""
  }

  render() {
    const { children } = this.props;

    // FIXME: onClick and highlight is not consistent
    return (
      <Layout className="layout" style={{ height: "100%" }}>
        <Header>
        <Menu
          theme="dark"
          mode="horizontal"
          openKeys={[this.state.selectedKey ? this.state.selectedKey : "home-page"]}
          style={{ lineHeight: '64px' }}
          onClick={({ key }) => this.setState({
            selectedKey: key
          })}
        >
          <Menu.Item key="home">
            <Link to={"/"}>
              MINT DT
            </Link>
          </Menu.Item> 
          <Menu.Item key="home-page" onClick={() => this.setState({
              selectedKey: "home-page"
            })}>
            <Link to={"/"} onClick={() => this.setState({
              selectedKey: "home-page"
            })}>
              Home
            </Link>
          </Menu.Item> 
          <Menu.Item key="adapters">
            <Link to={"/adapters"}>
              Adapters
            </Link>
          </Menu.Item> 
          <Menu.Item key="pipelines">
            <Link to={"/pipelines"}>
              Pipeline
            </Link>
          </Menu.Item>
          <Menu.Item key="create-pipeline">
            <Link to={"/pipeline/create"}>
              Create Pipeline
            </Link>
          </Menu.Item>
          <Menu.Item style={{ float: "right" }}>
            <Search
              placeholder="Search MINT DT!"
              onSearch={value => console.log(value)}
            />
          </Menu.Item>
        </Menu>
        </Header>
        <Content style={{ padding: '30px 50px' }}>
        <div style={{ background: '#fff', padding: 24, minHeight: 400, height: "100%", overflow: "auto" }}>
          {children}
        </div>
        </Content>
        <Footer style={{ textAlign: 'center', verticalAlign: "bottom" }}>MINT ©2020 Created by USC ISI</Footer>
      </Layout>
    );
  }
}