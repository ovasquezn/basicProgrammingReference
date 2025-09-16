# Datos de ejemplo
df <- data.frame(
  x = rnorm(100),
  y = rnorm(100)
)

# Crear el gráfico
p <- ggplot(df, aes(x, y)) +
  geom_point(color = "blue") +
  theme_minimal() +
  labs(title = "Gráfico de Dispersión", x = "Eje X", y = "Eje Y")

# Guardar en formato PNG y PDF
ggsave("plots/grafico_dispersion.png", plot = p, width = 6, height = 4, dpi = 300)
ggsave("plots/grafico_dispersion.pdf", plot = p)