# resource "aws_db_instance" "krmvalidatie" {
#   identifier_prefix      = "krmvalidatie-db-${terraform.workspace}"
#   allocated_storage      = 20
#   max_allocated_storage  = 100
#   db_name                = "postgres_krmvalidatie_${terraform.workspace}"
#   engine                 = "postgres"
#   engine_version         = "16.3"
#   instance_class         = "db.t3.micro"
#   username               = "krmvalidatie"
#   password               = random_password.db_password.result
#   vpc_security_group_ids = [aws_security_group.krmvalidatie-db.id]
#   db_subnet_group_name   = aws_db_subnet_group.krmvalidatie.name
#   publicly_accessible    = false
#   storage_type           = "gp2"
#   skip_final_snapshot    = true
#   multi_az               = false

# }

# resource "random_password" "db_password" {
#   length  = 24
#   special = false
# }

# resource "aws_secretsmanager_secret" "postgres_credentials" {
#   name = "krmvalidatie-postgres-credentials-${terraform.workspace}"
# }

# resource "aws_secretsmanager_secret_version" "postgres_credentials" {
#   secret_id = aws_secretsmanager_secret.postgres_credentials.id
#   secret_string = jsonencode({
#     username = aws_db_instance.krmvalidatie.username,
#     password = random_password.db_password.result
#   })
# }

# resource "aws_security_group" "krmvalidatie-db" {
#   name   = "krmvalidatie-db-sg-${terraform.workspace}"
#   vpc_id = aws_vpc.vpc.id

#   # ingress {
#   #   from_port       = 5432
#   #   to_port         = 5432
#   #   protocol        = "tcp"
#   #   security_groups = [aws_security_group.lambda_sg.id]
#   #   description     = "Allow requests from lambda"
#   # }


#   ingress {
#     from_port   = 5432
#     to_port     = 5432
#     protocol    = "tcp"
#     cidr_blocks = [aws_vpc.vpc.cidr_block]
#     description = "Allow requests from VPC"
#   }
# }

# resource "aws_db_subnet_group" "krmvalidatie" {
#   name       = "postgres-krmvalidatie-sn-group-${terraform.workspace}"
#   subnet_ids = [aws_subnet.az1a.id, aws_subnet.az1b.id, aws_subnet.az1c.id]

#   tags = {
#     Name = "DB subnet group for krmvalidatie database"
#   }
# }