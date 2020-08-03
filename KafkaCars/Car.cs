using Avro;
using Avro.Specific;
using Newtonsoft.Json;

namespace KafkaCars
{
    public class CarRecord : ISpecificRecord
    {
        public const string SchemaText = @"
                        {
                            ""fields"": [
                            {
                                ""name"": ""car_id"",
                                ""type"": ""string""
                            },
                            {
                                ""name"": ""manufacturer"",
                                ""type"": ""string""
                            },
                            {
                                ""name"": ""model"",
                                ""type"": ""string""
                            },
                            {
                                ""name"": ""model_year"",
                                ""type"": ""string""
                            }
                            ],
                            ""name"": ""CarRecord"",
                            ""namespace"": ""KafkaCars"",
                            ""type"": ""record""
                        }";
        public static Schema _SCHEMA = Schema.Parse(SchemaText);

        [JsonIgnore]
        public virtual Schema Schema => _SCHEMA;
        public string CarId { get; set; }
        public string Manufacturer { get; set; }
        public string Model { get; set; }
        public string ModelYear { get; set; }

        public virtual object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return this.CarId;
                case 1: return this.Manufacturer;
                case 2: return this.Model;
                case 3: return this.ModelYear;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            };
        }
        public virtual void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: this.CarId = (string)fieldValue; break;
                case 1: this.Manufacturer = (string)fieldValue; break;
                case 2: this.Model = (string)fieldValue; break;
                case 3: this.ModelYear = (string)fieldValue; break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            };
        }
    }
}