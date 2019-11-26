import * as React from "react";
import { Link } from "react-router-dom";
import {  Menu, Layout, Input } from "antd";
const { Header, Content, Footer } = Layout;
const { Search } = Input;

type LayoutProps = {
  children?: any,
};

export default class MyLayout extends React.Component<LayoutProps> {

  render() {
    const { children } = this.props;

    return (
      <Layout className="layout">
        <Header>
        <Menu
          theme="dark"
          mode="horizontal"
          defaultSelectedKeys={["home-page"]}
          style={{ lineHeight: '64px' }}
        >
          <Menu.Item key="home">
            <Link to={"/"}>
              MINT DT
            </Link>
          </Menu.Item> 
          <Menu.Item key="home-page">
            <Link to={"/"}>
              Home
            </Link>
          </Menu.Item> 
          <Menu.Item key="browse">
            <Link to={"/browse"}>
              Browse
            </Link>
          </Menu.Item> 
          <Menu.Item key="Pipeline">
            <Link to={"/pipeline"}>
              Pipeline
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
        <Content style={{ padding: '0 50px' }}>>
        <div style={{ background: '#fff', padding: 24, minHeight: 400 }}>
          {children}
        </div>
        </Content>
        <Footer style={{ textAlign: 'center', verticalAlign: "bottom" }}>Ant Design ©2018 Created by Ant UED</Footer>
      </Layout>
    );
  }
}