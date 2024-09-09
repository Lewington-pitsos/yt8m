provider "aws" {
  region = "us-east-1"
}

resource "aws_key_pair" "yt8m_deployer" {
  key_name   = "yt8m_deployer_key"
  public_key = file("~/.ssh/id_sache.pub") # NOTE: Change this to your public key path
}

resource "aws_security_group" "yt8m_allow_ssh" {
  name        = "yt8m_allow_ssh"
  description = "Allow SSH inbound traffic"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "yt8m" {
  ami           = "ami-0197c13a4f68c9360"
  instance_type = var.instance_type
  key_name      = aws_key_pair.yt8m_deployer.key_name

  security_groups = [aws_security_group.yt8m_allow_ssh.name]

  tags = {
    Name = "yt8m"
  }

  root_block_device {
    volume_size = 400
  }
}